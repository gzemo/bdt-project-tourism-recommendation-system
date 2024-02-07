import os
import random
import pandas as pd
from termcolor import colored, cprint
pathToFolder = "/bdtproject"
os.sys.path.append(pathToFolder)

from run.utils import *
from run.similarity import SimiliarityManager
from run.recommend import Recommender
from run.statistics import ComputeStatistics
from dbconnector.spark_connector import SparkConnector
from dbconnector.redis_connector import RedisConnector
from dbconnector.postgres_connector import PostgresConnector
from dbconnector.cassandra_connector import CassandraConnector
from POI.common import POIparser
from POI.OSMparser import OSMparser
from POI.municipality_parser import MuncipalityParser
from POI.selection_POI import SelectionPOI
from clients.common import User, OldUser

# --- services parameters: ---
SPARK_PS = ""
POSTGRESS_PS = ""

# --- simulation parameters: ---
COMPLETE_SIMULATION          = True # run simulation from beginning; otherwise simulate the user request 
PARSING_FROM_CASSANDRA       = False # whether to perform the Overpass API request, store into cassandra and filter to postgress
PARSING_FROM_GEOCODING       = False # whether to retrieve the municipality's coordinates from (reverse) geocoding maps
N_BASELINE_USERS             = 300 # number of baseline user to fill as starting entries
N_SIMULATED_REQUEST          = 15  # number of request receiving at the end of the simulation
PROBABILITY_NEW_USER_COMING  = 0.5 # probability of having a simulating user being a new, unregistered one
N_POI_TO_RETURN              = 100  # number of final POI suggestion to be retained
INITIAL_USER_DISTANCE        = 10 # initial radius from where to look while filtering for the best recommended POI
PAST_USER_ATTEMPTS           = 5 # changing locations attempts while dealing with underepresented trip's categories
NEW_USER_SIMILARITY_TRESHOLD = 0.10 # max similarity threshold
NEW_USER_MIN_N_NEIGHBOURS    = 15 # minimum number of similar user from which take the averaged ratings
INITIAL_USER_DISTANCE        = 10 # initial radius from where to look to filter the recommended POI
MAX_USER_DISTANCE            = 100 # maximum allowed radius to find POIs
NEW_USER_GAP_TO_UPDATE       = 3 # triggering model re-train after how much new user registered



def initialize_spark_connector():
	return SparkConnector(
				pathToJarFile="./dbconnector/postgresql-42.6.0.jar",
				masterName="local",
				userName="postgres",
				password=SPARK_PS, 
				driver="org.postgresql.Driver")

def initialize_redis_connector():
	return RedisConnector(
				hostname="localhost", 
				port=6380)

def initialize_postgres_connector():
	return PostgresConnector(
				dbname="bdt", 
				hostname="localhost",
				username="postgres",
				password=POSTGRESS_PS,
				port="5433")

def initialize_cassandra_connector():
	return CassandraConnector(
			 	contact_points=['0.0.0.0'], 
				port="9042",
				keyspace_name="bdt", 
				table_name="rawpoi",
				initialize = False)


def initialize_user_baseline(Nusers:int=500, userSeed:int=1):
		"""
		Wrapper function that simulates the initial user's baseline
		Args:
			Nusers: (int) number of user to simulate to populate the baseline
			userSeed: (int) current user seed for replicability
		"""
		random.seed(userSeed)
		printBold("[User simulation]: ", "blue", space=0, toend="")
		printNorm("starting simulating users baseline", "white", space=0, toend="\n")	
		for u in range(Nusers): 
			if (u+1) % 100 == 0:
				printNorm(f".................: Users simulated: ", "white", space=0, toend="")
				printNorm(f"{u+1}", "green", space=0, toend="\n")
			randomDest, ratings = [], []
			user = User(postgres_connector, redis_connector)
			nTrip = random.randint(1, 30)
			for i in range(nTrip):
				randomDest.append(random.choice(poi_names))
				ratings.append(fair_ratings())
			user.update_storages(categories, randomDest, ratings)
		printBold("[User simulation]: ", "blue", space=0, toend="")
		printNorm("Done!", "green", space=0, toend="\n")


if __name__ == "__main__":

	assert (SPARK_PS!="" or POSTGRESS_PS!=""), "Services psw not initialized yet! (Check main.py)"


	### initialize connectors
	spark_connector = initialize_spark_connector()

	redis_connector = initialize_redis_connector()

	postgres_connector = initialize_postgres_connector()

	if COMPLETE_SIMULATION:
		###  (eventually) clean and initialize database
		drop_postgres_database_poi(postgres_connector)
		initialize_postgres_database_poi(postgres_connector)
	 
		drop_postgres_database_municipalities(postgres_connector)
		initialize_postgres_database_municipalities(postgres_connector)

		drop_postgres_database_user(postgres_connector)
		initialize_postgres_database_user(postgres_connector)

		drop_postgres_database_ratings(postgres_connector)
		initialize_postgres_database_ratings(postgres_connector)

		redis_connector.clear_current_workspace()

		### request from Open Street Map by Overpass into Cassandra
		open_street_map_parser = OSMparser(
					postgres_connector, 
					redis_connector, 
					spark_connector,
					cassandra_connector=None, 
					blocksFileName="./POI/overpass_query_block.txt")

		poi_parser = POIparser(
					postgres_connector,
					cassandra_connector=None)

		munici_parser = MuncipalityParser(
					postgres_connector,
					"Trentino-Alto Adige/Südtirol")

		### Performing request to the Overpass API and store into Cassandra Cluster

		cql = """SELECT * FROM bdt.rawpoi WHERE block_name = ? ALLOW FILTERING;"""

		for block in open_street_map_parser.blocks_name:
			if PARSING_FROM_CASSANDRA:
				poi_parser.store_into_postgres_single(cql, block)
			else:
				jsonfile = os.path.join("./POI/OpenStreetMapTmpStorage", f"{block}.json")
				poi_parser.store_into_postgres_single_from_json(jsonfile)

		if PARSING_FROM_GEOCODING:
			munici_parser.parse_dataset("./POI/municipalities_of_trentino.txt", verbose=True)
		else:
			munici_parser.parse_dataset_from_file("./POI/municipalities.csv", verbose=True)


	### retrieve all currently available categories/possible destinations and poi names
	categories   = get_all_unique_categories(postgres_connector)
	destinations = get_all_unique_destinations(postgres_connector)
	poi_names    = get_all_unique_poi(postgres_connector)

	if COMPLETE_SIMULATION:
		### multiple user test (Baseline) (this may take a while...)
		initialize_user_baseline(Nusers=N_BASELINE_USERS, userSeed=1)
					
	### retrieve now all Users id
	allUsers = get_all_unique_user_spark_id(postgres_connector)

	### recommender-related initialization
	sm = SimiliarityManager(spark_connector, redis_connector)
	rec = Recommender(spark_connector, redis_connector, postgres_connector,	sm)
	rec.initialize_similarity_manager()
	rec.initialize_recommender()
	selectPOI = SelectionPOI(postgres_connector)

	printBold("\n[User simulation]: ", "blue", space=0, toend="")
	printNorm("starting simulating users' request: ", "white", space=0, toend="\n")

	random.seed(0)
	for _ in range(N_SIMULATED_REQUEST):
		print()
		outcome = random.random()
		if outcome > PROBABILITY_NEW_USER_COMING:

			printBold("[User simulation]: ", "blue", space=0, toend="")
			printBold("Past user ", "green", space=0, toend="\n")

			allUsers = get_all_unique_user_spark_id(postgres_connector)
			old_user_id = random.choice(allUsers)
			old_user_destination = random.choice(destinations)

			attempt = 0
			while attempt < PAST_USER_ATTEMPTS:

				old_user = OldUser(postgres_connector,
									redis_connector,
									old_user_id,
									old_user_destination,
									categories)

				printNorm("Current user id:  ", "white", space=3, toend="")
				printNorm(f"{old_user.get_spark_id()}", "cyan", space=0, toend="\n")
				printNorm("Destination choosen:  ", "white", space=3, toend="")
				printNorm(f"{old_user.get_destination()}", "cyan", space=0, toend="\n")
				printNorm("Set of user preferences:  ", "white", space=3, toend="")
				printNorm(f"{old_user.get_categories()}", "cyan", space=0, toend="\n")
				top_pois_names, top_poi_predicted_ratings = rec.predict_old_user(old_user.get_spark_id(), toReturn=N_POI_TO_RETURN)
				try:
					selectPOI.add_current_pois(top_pois_names)
					selectPOI.add_current_destination(old_user.get_destination())
					list_dist, list_cat = selectPOI._build_columns()
					closest = selectPOI.convergence_select_poi(
								df=pd.DataFrame(data={"name":top_pois_names,"type":list_cat,"distance":list_dist}),
								user_categories=old_user.get_categories(),
								user_distance=INITIAL_USER_DISTANCE,
								max_distance=MAX_USER_DISTANCE,
								toReturn=N_POI_TO_RETURN)
				except:
					closest = []
				#closest = selectPOI.convergence_select_poi(user_categories=old_user.get_categories(),
				#								user_distance=INITIAL_USER_DISTANCE,
				#								max_distance=MAX_USER_DISTANCE,
				#								toReturn=N_POI_TO_RETURN)
	
				if closest!=[]:
					break
				else:
					printBold("[Update]: ", "blue", space=3, toend="")
					printNorm("Changing trip's destination:  ", "green", space=0, toend="\n")
					old_user_destination = random.choice(destinations)
					attempt += 1
					
			if closest==[]:
				printBold("[WARNING]: ", "red", space=6, toend="", attrs=["bold", "dark"])
				printNorm(f"after {attempt} attempt, no POI is found according to user categories!", "white", space=0, toend="\n")
				continue

			choice = random.choice(closest)
			rate   = fair_ratings()
			old_user.store_rating_postgres(choice, rate)
			printBold("[Storage]: ", "green", space=6, toend="", attrs=["bold", "dark"])
			printNorm("Receiving reviews on:", "white", space=0, toend="\n")
			printNorm(f".........: {choice}, {rate} stars!", "cyan", space=6, toend="\n")
			printBold("[Storage]: ", "green", space=6, toend="", attrs=["bold", "dark"])
			printNorm("Review complete: user stored!", "white", space=0, toend="\n")

		else:
			
			printBold("[User simulation]: ", "blue", space=0, toend="")
			printBold("New user ", "green", space=0, toend="\n")

			new_user = User(postgres_connector, redis_connector)
			new_user.store_profile_attributes_postgres()
			new_user.simulate_from_database()
			new_user.store_preferences_attributes_redis(categories)
			
			printNorm("Current user id:  ", "white", space=3, toend="")
			printNorm(f"{new_user.get_spark_id()}", "cyan", space=0, toend="\n")
			printNorm("Destination choosen:  ", "white", space=3, toend="")
			printNorm(f"{new_user.get_destination()}", "cyan", space=0, toend="\n")

			#  change the preferences
			while True:
				printNorm("User's trip categories:  ", "white", space=3, toend="")
				printNorm(f"{new_user.get_categories()}", "cyan", space=0, toend="\n")
				top_pois_names, top_poi_predicted_ratings = rec.predict_new_user( 
								user_spark_id=new_user.get_spark_id(), 
								destination=new_user.get_destination(),
								threshold=NEW_USER_SIMILARITY_TRESHOLD,
								minNeighbours=NEW_USER_MIN_N_NEIGHBOURS,
								toReturn=N_POI_TO_RETURN)

				selectPOI.add_current_pois(top_pois_names)
				selectPOI.add_current_destination(old_user.get_destination())
				list_dist, list_cat = selectPOI._build_columns()
				closest = selectPOI.convergence_select_poi(
							df=pd.DataFrame(data={"name":top_pois_names,"type":list_cat,"distance":list_dist}),
							user_categories=old_user.get_categories(),
							user_distance=INITIAL_USER_DISTANCE,
							max_distance=MAX_USER_DISTANCE,
							toReturn=N_POI_TO_RETURN)
				
				if closest == []: # exit condition
					printNorm("Changing trip's categories:  ", "green", space=3, toend="\n")
					new_user.update_preferences_attributes_redis(categories)
				else:
					break
			# random choice + storaging
			choice = random.choice(closest)
			rate   = fair_ratings()
			new_user.store_rating_postgres(choice, rate)
			printBold("[Storage]: ", "green", space=6, toend="", attrs=["bold", "dark"])
			printNorm("Receiving reviews on:", "white", space=0, toend="\n")
			printNorm(f".........: {choice}, {rate} stars!", "cyan", space=6, toend="\n")
			printBold("[Storage]: ", "green", space=6, toend="", attrs=["bold", "dark"])
			printNorm("Review complete: user stored!", "white", space=0, toend="\n")
			rec.update_recommender_schedule(delay=NEW_USER_GAP_TO_UPDATE)

	### initialize statistics
	cs = ComputeStatistics(postgres_connector)
	cs.print_all()
