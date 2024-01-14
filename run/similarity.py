import pyspark.sql.functions as f
from pyspark.ml.linalg import Vectors as v
from pyspark.ml.feature import MinHashLSH
from run.utils import printBold, printNorm
from dbconnector.spark_connector import SparkConnector
from dbconnector.redis_connector import RedisConnector



class SimiliarityManager():

	def __init__(self, spark_connector, redis_connector):
		self.spark_connector = spark_connector
		self.redis_connector = redis_connector
		self.base_df = None
		self.isInitialized = False
		self.model = None  # initializing the MinHash model fitted over the base_df

	
	def _get_iterator(self, prefix="*:preferences"):
		"""
		Return the iterable object to be parsed while loading data 
		into a DataFrame
		Args:
			prefix: (str) to be returned 
		"""
		return self.redis_connector.scan_keys(prefix)


	def _retrieve_preferences(self, user_spark_id, nItems=10000):
		""" 
		Return:
			list of POI visited ???? already converted in list of integers
			by a given user spark id key
		"""
		
		access_key = user_spark_id if "preferences" in str(user_spark_id) else f"{user_spark_id}:preferences"
		tmp = self.redis_connector.retrieve(access_key)
		if type(tmp[0])==str:
			tmp = [int(tmp[i]) for i,_ in enumerate(tmp)]
		return tmp


	def _convert_preferences2sparkdense(self, user_spark_id, user_preferences):
		"""
		Args:
			user_spark_id: (str) user designed spark id (ex: "0:preferences")
			user_preferences: (list) of binary preferences
		Return:
			A single tuple of (id, sparkVectors.dense(preferences))
		"""
		#return (int(user_spark_id[0]), v.dense([user_preferences[i] for i,_ in enumerate(user_preferences)]))
		
		key_clean = int(user_spark_id.split(":")[0])
		return tuple((key_clean, v.dense(user_preferences)))

		
	def build_query_dataframe(self, query_spark_id):
		""" 
		Build and return the id. features spark dataframe by a given query_spark_id
		"""
		user_preferences = self._retrieve_preferences(query_spark_id)
		current_tuple = self._convert_preferences2sparkdense(query_spark_id, user_preferences)
		return self.spark_connector.createDF([current_tuple], ["id", "features"])


	def initialize_base_dataframe(self):
		"""
		Generate the first base dataframe and store as attribute
		"""
		self.isInitialized = True
		self.build_base_dataframe(update=True)


	def build_base_dataframe(self, update=True, verbose=True):
		"""
		Retrieve the information of preferences from Redis in order
		to build the base DataFrame for the similarity computation
		Args:
			update: (bool) whether to update the current spark DataFrame of the baseline
		Return:
			spark DataFrame of id and feature vectors
		"""
		if update:
			if verbose:
				printBold("[Processing]: ", "cyan", space=0, toend="")
				printNorm("building preferences DataFrame from Redis", "white", space=0, toend="\n")
			base_data = []
			all_preferences = self.redis_connector.scan_keys("*:preferences")
			for item in all_preferences:
				currentIndex = int(item.split(":")[0])
				current_tuple = ( currentIndex, v.dense(self._retrieve_preferences(currentIndex)) )
				base_data.append( current_tuple ) 
			self.base_df = self.spark_connector.createDF(base_data, ["id", "features"])
		printBold("[Processing]: ", "cyan", space=0, toend="")
		printNorm("Done!", "green", space=0, toend="\n")
		return self.base_df
	

	def fit_model(self, tmpSeed=1, verbose=True):
		"""
		Fit MinHash to the base dataframe
		"""
		if not self.base_df:
			raise Exception("Base DataFrame not initialized yet!")

		if verbose:
			printBold("[Recommend]: ", "yellow", space=0, toend="")
			printNorm("Fitting similarity by Spark MinHashLSH", "white", space=0, toend="\n")
		mh = MinHashLSH() \
			.setNumHashTables(10) \
			.setInputCol("features") \
			.setOutputCol("hashes") \
			.setSeed(tmpSeed)

		model = mh.fit(self.base_df).setInputCol("features")
		model.transform(self.base_df) #.head()
		self.model = model


	def get_similarities(self, query_df, threshold=0.10, verbose=False):
		"""
		Estimate the most similar users according to a given similarity treshold
		Args:
			query_df: (Spark.DataFrame) dataframe of query preferences
				format: 
			threshold: (float) (min=0, max=1) consider all values below that threshold
		Return a list of tuple according to the closest users by means of Jaccard Distance
			and the current threshold
		"""
		if not self.isInitialized:
			raise Exception("Base DataFrame not built yet")
		if not self.model:
			raise Exception("Model has not been fit yet!")

		similar = self.model.approxSimilarityJoin(query_df, self.base_df, threshold, distCol="JaccardDistance") \
			.select(f.col("datasetA.id").alias("idA"), f.col("datasetB.id").alias("idB"), f.col("JaccardDistance"))
		
		if verbose:
			similar.show()
		similar_users_id = similar.select(f.collect_list("idB")).first()[0]
		similar_users_distance = similar.select(f.collect_list("JaccardDistance")).first()[0]
		assert len(similar_users_id) == len(similar_users_distance), "Something went wrong: length mismatch!"
		return sorted([(similar_users_distance[i], similar_users_id[i]) for i,_ in enumerate(similar_users_distance)])
	

	def convergence_similarities(self, query_df, threshold, toReturn, verbose=True):
		""" 
		This is a wrapper of the "get_similaries" function which returns the list of tuples
		according to the closest user by checking the minimum number of allowed neighbours.
		It itiratively increases the initial threshold to find a sufficient number of user which 
		satisfies the similarity.

		Args:
			query_df: (Spark.DataFrame) dataframe of query preferences
				format: 
			threshold: (float) (min=0, max=1) consider all values below that threshold
			toReturn: (int), minimum number of similar user to consider
		Return:
			similar: (list) a list of tuple according to the closest users by means of Jaccard Distance
				and the current threshold
			threshold: (float) the final threshold according to which the toReturn user is respected

		"""
		similar = self.get_similarities(query_df, threshold=threshold)
		while len(similar) < toReturn:
			threshold += 0.05
			if verbose:
				printBold("[Similarity]: ", "green", space=3, toend="")
				printNorm("Not enough similar users found: testing thr = ", "white", space=0, toend="")
				printBold(f"{round(threshold,2)}", "magenta", space=0, toend="\n")
			similar = self.get_similarities(query_df, threshold=threshold)
		return similar, threshold