import os
import sys
import random
from termcolor import colored, cprint

# --- General Utils: ---

def initialize_postgres_database(postgres_connector, verbose=True):
	"""
	Initializing postgres relation
	"""
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Initializing All Postgres relation ", "white", space=0, toend="\n")

	sql1 = '''CREATE TABLE USERS(
		user_spark_id integer, user_id varchar, user_name varchar,
		user_surname varchar, user_sex varchar, user_birth date
		)''';
	postgres_connector.execute(sql1)
	
	sql3 = '''CREATE TABLE POI(poi_spark_id bigint,
							poi_id bigint, 
							lat numeric,
							lon numeric, 
							poi_type varchar, 
							poi_name varchar, 
							poi_place varchar,
							poi_addr varchar)''';
	postgres_connector.execute(sql3)

	sql4 = '''CREATE TABLE RATINGS(user_spark_id integer, poi_spark_id integer, rating smallint)''';	
	postgres_connector.execute(sql4)

	sql5 = '''CREATE TABLE MUNICIPALITIES(mun_name varchar, mun_lat float, mun_lon float)''';
	postgres_connector.execute(sql5)

	sql6 = '''CREATE TABLE TOP_POI(poi_name varchar, poi_type varchar, poi_distance float)''';
	postgres_connector.execute(slq6)


def drop_postgres_database(postgres_connector):
	"""
	Remove USERS, PREFERENCES POI and RATINGS relation 
	from the current postgres_connector
	"""
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting All Postgres relations ", "red", space=0, toend="\n")
	sql1 = '''DROP TABLE USERS''';
	postgres_connector.execute(sql1)

	sql3 = '''DROP TABLE POI''';
	postgres_connector.execute(sql3)

	sql4 = '''DROP TABLE RATINGS''';
	postgres_connector.execute(sql4)

	sql5 = '''DROP TABLE MUNICIPALITIES''';
	postgres_connector.execute(sql5)

	sql6 = '''DROP TABLE TOP_POI''';
	postgres_connector.execute(sql6)


def initialize_postgres_database_user(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Initializing Postgres USER relation ", "white", space=0, toend="\n")
	sql = '''CREATE TABLE USERS(
		user_spark_id integer, user_id varchar, user_name varchar,
		user_surname varchar, user_sex varchar, user_birth date
		)''';
	postgres_connector.execute(sql)

def drop_postgres_database_user(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting Postgres USER relations ", "red", space=0, toend="\n")
	sql = '''DROP TABLE USERS''';
	try:
		postgres_connector.execute(sql)
	except:
		pass

def initialize_postgres_database_poi(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Initializing Postgres POI relation ", "white", space=0, toend="\n")
	sql = '''CREATE TABLE POI(poi_spark_id bigint,
							poi_id bigint, 
							lat numeric,
							lon numeric, 
							poi_type varchar, 
							poi_name varchar, 
							poi_place varchar,
							poi_addr varchar)''';
	postgres_connector.execute(sql)

def drop_postgres_database_poi(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting Postgres POI relations ", "red", space=0, toend="\n")
	sql = '''DROP TABLE POI''';
	try:
		postgres_connector.execute(sql)
	except:
		pass

def initialize_postgres_database_ratings(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Initializing Postgres RATINGS relation ", "white", space=0, toend="\n")
	sql = '''CREATE TABLE RATINGS(user_spark_id integer, poi_spark_id integer, rating smallint)''';	
	postgres_connector.execute(sql)

def drop_postgres_database_ratings(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting Postgres RATINGS relation ", "red", space=0, toend="\n")
	sql = '''DROP TABLE RATINGS''';
	try:
		postgres_connector.execute(sql)
	except:
		pass	

def initialize_postgres_database_municipalities(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Initializing Postgres MUNICIPALITIES relation ", "white", space=0, toend="\n")
	sql = '''CREATE TABLE MUNICIPALITIES(mun_name varchar, mun_lat float, mun_lon float)''';
	postgres_connector.execute(sql)

def drop_postgres_database_municipalities(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting Postgres MUNICIPALITIES relations ", "red", space=0, toend="\n")
	sql = '''DROP TABLE MUNICIPALITIES''';
	try:
		postgres_connector.execute(sql)
	except:
		pass

def initialize_postgres_database_top_poi(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Initializing Postgres TOP_POI relation ", "white", space=0, toend="\n")
	sql = '''CREATE TABLE TOP_POI(poi_name varchar, poi_type varchar, poi_distance float)''';
	postgres_connector.execute(sql)

def drop_postgres_database_top_poi(postgres_connector):
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting Postgres TOP_POI relation ... ", "red", space=0, toend="\n")
	sql = '''DROP TABLE TOP_POI''';
	try:
		postgres_connector.execute(sql)
	except:
		pass	

def initialize_cassandra_keyspace(cassandra_connector):
	"""
	Initialize the cassandra_connection and generate the keyspace
	(Hardcoded!)
	"""
	# key space
	cassandra_connector.execute(
		"""CREATE KEYSPACE bdt WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"""
		)
	# tables
	cassandra_connector.execute(
		"""CREATE TABLE bdt.rawpoi (id bigint,lat decimal,lon decimal,block_name text,tags map<text, text>,PRIMARY KEY ((id, block_name)));"""
		)

def drop_cassandra_keyspace(cassandra_connector):
	"""
	Remove the current bdt keyspace
	(Hardcoded!)
	"""
	printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
	printNorm("Deleting Cassandra BDT Keyspace ", "red", space=0, toend="\n")
	cassandra_connector.execute("""DROP KEYSPACE bdt;""")

def get_all_unique_user_spark_id(postgres_connector):
	"""
	Return the list of all distinc POI spark id
	"""
	sql_users = '''SELECT DISTINCT user_spark_id FROM USERS''';
	users = postgres_connector.execute(sql_users)
	return [el[0] for el in users]

def get_all_unique_destinations(postgres_connector):
	"""
	Return all unique destinations querying "municipality name" form MUNICIPALITIES relation
	"""
	sql_dest = '''SELECT DISTINCT mun_name FROM MUNICIPALITIES''';
	destinations = postgres_connector.execute(sql_dest)
	return [el[0] for el in destinations]

def get_all_unique_categories(postgres_connector):
	"""
	Return all unique POI type: categories that can must be choosen or not
	querying "poi_type" form POI relation
	"""
	sql_cat = '''SELECT DISTINCT poi_type FROM POI''';
	categories = postgres_connector.execute(sql_cat)
	return [el[0] for el in categories]

def get_all_unique_poi(postgres_connector):
	"""
	Return all unique POI name
	"""
	sql_poi = '''SELECT DISTINCT poi_name FROM POI''';
	poi_names = postgres_connector.execute(sql_poi)
	return [el[0] for el in poi_names]


# --- Printing functions: ---

def fair_ratings():
	return random.choices([1,2,3,4,5], weights=[0.10,0.10,0.20,0.40,0.20])[0]

def printBold(string_to_print, color, space=0, toend="\n", attrs=["bold"]):
	initial_space = " "*space
	cprint(initial_space+string_to_print, color, attrs=attrs, file=sys.stderr, end=toend)

def printNorm(string_to_print, color, space=0, toend="\n"):
	initial_space = " "*space
	cprint(initial_space+string_to_print, color, file=sys.stderr, end=toend)
