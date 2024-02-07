import time
import pyspark.sql.functions as f
from pyspark.ml.recommendation import ALS
from run.utils import printBold, printNorm
from run.similarity import SimiliarityManager
from POI.selection_POI import SelectionPOI



class Recommender():

	def __init__(self, spark_connector, redis_connector, postgres_connector,
		similarity_manager):
		self.spark_connector = spark_connector
		self.redis_connector = redis_connector
		self.postgres_connector = postgres_connector
		self.similarity_manager = similarity_manager
		self.ratings_df = None
		self.model = None
		self.isModelComputed = False
		self.currNumUsers = 0


	def _is_model_computed(self):
		"""
		Check if the model has been perfromed at least once
		"""
		return self.isModelComputed

	def _check_current_nUsers(self):
		"""
		Return the nuber of current users
		"""
		sql_users = '''SELECT DISTINCT user_spark_id FROM USERS''';
		users = self.postgres_connector.execute(sql_users)
		return len(users)


	### -----------------------------------------------------------------------------
	### similarity managment functions 
	### -----------------------------------------------------------------------------

	def initialize_similarity_manager(self):
		"""
		Initialize the base DataFrame of the preferences (from Redis)
		"""
		self.similarity_manager.initialize_base_dataframe()
		self.similarity_manager.fit_model()

	def update_similarity_manager(self):
		"""
		Update the base preferences DataFrame and re-fit the MinHash model 
		over the newly built preferences
		"""
		self.similarity_manager.build_base_dataframe(update=True)
		self.similarity_manager.fit_model()


	### -----------------------------------------------------------------------------
	### Model computation functions 
	### -----------------------------------------------------------------------------

	def get_table(self, table:str="ratings"):
		"""
		Connect Spark to postgress: reading from postrgreSQL rating relation 
		to save as spark Dataframe to be run before ALS!
		Return:
			spark dataframe of table (default: ratings)
		"""
		try:
			dbname = self.postgres_connector.get_dbname()
			username = self.postgres_connector.get_username()
			password = self.postgres_connector.get_password()
			port = self.postgres_connector.get_port()

			connector = self.spark_connector.get_connector()
			self.ratings_df = connector.read \
				.format("jdbc") \
				.option("url", f"jdbc:postgresql://localhost:{port}/{dbname}") \
				.option("dbtable", table) \
				.option("user", username) \
				.option("password", password) \
				.option("driver", "org.postgresql.Driver") \
				.load()
		except:
			raise Exception(f"Can not load current {table} table")
		return self.ratings_df

	def fit_ALS(self, userCol:str="user_spark_id", itemCol:str="poi_spark_id",
				ratingCol:str="rating",	coldStartStrategy:str="drop", verbose:bool=True):
		"""
		Fit Alternating Least Squares over the currently loaded DataFrame of ratings 
		(which needs to be loaded first)
		Return:
			ALS model objecet fitted over base_df
		"""
		# initial check to detect whether the rating df has been loaded properly
		if not self.ratings_df:
			self.get_table()

		if verbose:
			printBold("[Recommend]: ", "yellow", space=0, toend="")
			printNorm("now running ALS for predictions ...", "white", space=0, toend="\n")
		
		als = ALS(rank=10, seed=0, maxIter=10, regParam=0.1, 
			coldStartStrategy=coldStartStrategy,
			userCol=userCol, itemCol=itemCol, ratingCol=ratingCol)

		self.model = als.fit(self.ratings_df)
		self.model.setPredictionCol("newPrediction")
		self.isModelComputed = True

		if verbose:
			printBold("[Recommend]: ", "yellow", space=0, toend="")
			printNorm("Done!", "green", space=0, toend="\n")
		
		return self.model


	### -----------------------------------------------------------------------------
	### Update scheduler
	### -----------------------------------------------------------------------------

	def initialize_recommender(self, table:str="ratings"):
		"""
		Initially, the ratings DataFrame build and ALS models are loaded and computed
		respectively to the pool baseline of users.
		"""
		self.currNumUsers = self._check_current_nUsers()
		self.get_table(table)
		self.fit_ALS()


	def update_recommender_schedule(self, delay:int=50, table:str="ratings",
		verbose:bool=True):
		"""
		Args:
			delay: (int) how many new users are needed in order to refit the model
		"""
		tmp = self.currNumUsers
		current = self._check_current_nUsers()
		if current - tmp >= delay:
			if verbose:
				printBold("\n[Updating]: ", "magenta", space=0, toend="")
				printNorm("updating schedule", "white", space=0, toend="\n")
				printBold("[Processing]: ", "cyan", space=0, toend="")
				printNorm("update baseline pool of users", "white", space=0, toend="\n")
			self.currNumUsers = self._check_current_nUsers()
			self.update_similarity_manager()
			self.get_table(table) # update current table and load the latest into a dataframe
			self.fit_ALS()        # fit the model over the newly-loaded dataframe


	def time_scheduler(self):
		"""
		Update by time 
		"""
		raise NotImplementedError


	### -----------------------------------------------------------------------------
	### Prediction 
	### -----------------------------------------------------------------------------


	def extract_not_visited_tuples(self, redis_user_id):
		"""
		Args:
			user:
			redis_user_id: (int) sparkUser is (0, 1, ...)
		Return:
			list tuples for all the pois not visited by the user
		"""

		# we need to retrieve all the pois avaiable from the POI dataset
		sql = '''SELECT DISTINCT poi_spark_id FROM POI;'''
		tmp_pois = self.postgres_connector.execute(sql)
		user_pois_visited = self.redis_connector.retrieve(redis_user_id)
		
		all_pois = set([el[0] for el in tmp_pois])
		user_pois_visited = set([int(el) for el in user_pois_visited])

		#id_user = user["sparkID"]
		poi_not_visited = all_pois - user_pois_visited
		res = [(redis_user_id, el) for el in poi_not_visited]
		return  self.spark_connector.createDF(res, ["user_spark_id", "poi_spark_id"]), res


	def predict_old_user(self, old_user, toReturn=10, verbose=True):
		"""
		Predict unseen POI's ratings for an old user (someone who has already rated some POI)
		old_user: (int) sparkUser is (0, 1, ...)
		Return:
			top_poi_names: list of already sorted top N POI
			top_poi_predicted_ratings: list of corresponding ratings
		"""
		if not self._is_model_computed():
			raise Exception("Recommender Model needs to be computed first!")

		if verbose:
			printBold("[Recommend]: ", "yellow", space=3, toend="")
			printNorm("not visited POI's rating prediction", "white", space=0, toend="\n")

		old_user_df, _ = self.extract_not_visited_tuples(old_user)
		predictions = sorted(self.model.transform(old_user_df).collect(), key=lambda r: r[2], reverse=True)
		top = predictions[0:toReturn]
		
		top_poi_spark_id          = [item["poi_spark_id"] for item in top]
		top_poi_predicted_ratings = [item["newPrediction"] for item in top]

		top_poi_names = []
		for poi_spark_id in top_poi_spark_id:
			sql = '''SELECT poi_name FROM poi WHERE poi_spark_id=%s''';
			current_poi_name = self.postgres_connector.execute(sql, (poi_spark_id,))
			top_poi_names.append(current_poi_name[0][0])

		if verbose:
			printBold("[Recommend]: ", "yellow", space=3, toend="")
			printNorm("Done!", "green", space=0, toend="\n")
		
		return top_poi_names, top_poi_predicted_ratings


	def predict_new_user(self, user_spark_id, destination, threshold=0.10,
		minNeighbours=20, toReturn=50, verbose=True):
		"""
		Args:
			user_spark_id: (int) 
			destination: (str)
			threshold: (float) initial Jaccard distance threshold allowed
			minNeighbours: (int) 
			toRetutn: (int) number of output to be returned
			verbose: (bool) whether to print intermediate results
		Return:
			top_poi_names (list) of most recommended POI's names filtered over the user destination
			 top_poi_predicted_ratings: (list) of corresponding POI's rating
			  X nUsers: (int) number of users found to be similar
			  X predictions_df_mean_sorted: (spark DataFrame), complete spark DataFrame
		"""	
		query_df = self.similarity_manager.build_query_dataframe(f"{user_spark_id}:preferences")
		
		similarity_list, final_threshold = self.similarity_manager.convergence_similarities(
			query_df, threshold=threshold, toReturn=minNeighbours, verbose=verbose)
		
		similarity_users = sorted([similarity_list[i][1] for i in range(len(similarity_list))])
		nUsers = len(similarity_users)

		if verbose:
			printBold("[Recommend]: ", "yellow", space=3, toend="")
			printNorm(f"Found {nUsers} similar users, according to similarity thr: {round(final_threshold, 2)}", "white", space=0, toend="\n")


		## we need to retrieve all the pois avaiable from the POI dataset
		sql = '''SELECT DISTINCT poi_spark_id FROM POI;'''
		tmp_pois = self.postgres_connector.execute(sql)
		all_pois = sorted([el[0] for el in tmp_pois])

		data = []
		for _,user in enumerate(similarity_users):
			for j in range(len(all_pois)):
				data.append((user, all_pois[j]))
		
		if verbose:
			printBold("[Recommend]: ", "yellow", space=3, toend="")
			printNorm(f"predicting user ratings", "white", space=0, toend="\n")

		neighbours_df  = self.spark_connector.createDF(data, ["user_spark_id", "poi_spark_id"])
		predictions    = self.model.transform(neighbours_df).collect()
		predictions_df = self.spark_connector.createDF(predictions, ["user_spark_id", "poi_spark_id", "newPrediction"])
		
		predictions_df_mean = predictions_df.groupBy("poi_spark_id").mean().select(["poi_spark_id", "avg(newPrediction)"])
		predictions_df_mean_sorted = predictions_df_mean.orderBy(predictions_df_mean["avg(newPrediction)"].desc())
		id_poi_col = predictions_df_mean_sorted.select(f.collect_list("poi_spark_id")).first()[0]
		avg_ratings_col = predictions_df_mean_sorted.select(f.collect_list("avg(newPrediction)")).first()[0]

		top_id = id_poi_col[0:toReturn]
		top_poi_names, top_poi_predicted_ratings = [],[]
		for i, poi_spark_id in enumerate(top_id):
			sql = '''SELECT poi_name FROM poi WHERE poi_spark_id=%s''';
			current_poi_name = self.postgres_connector.execute(sql, (poi_spark_id,))
			top_poi_names.append(current_poi_name[0][0])
			top_poi_predicted_ratings.append(avg_ratings_col[i])

		return top_poi_names, top_poi_predicted_ratings
