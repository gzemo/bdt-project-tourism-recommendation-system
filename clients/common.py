import uuid
import psycopg2
from faker import Faker


class User():

	idSpark = 0
	
	def __init__(self, postgres_connector, redis_connector, baseline:bool=True):
		self.postgres_connector = postgres_connector
		self.redis_connector    = redis_connector
		self.baseline = baseline
		self.faker = Faker("it_IT")
		self._user = dict()
		self._user["ID"] = str(uuid.uuid4()) #this is a unique random value of 32 elements to generate UNIQUE users
		self._user["sparkID"] = User.idSpark
		self._user["sex"] = self.faker.random_element(elements=('m', 'f')) #we assign a sex to the user
		self._user["name"] = self.faker.first_name_male() if self._user["sex"] == "m" else self.faker.first_name_female()
		self._user["surname"] = self.faker.last_name().replace("'", " ") #last names are assigned randomly (we don't need differentiation based on sex)
		self._user["dateofbirth"] = str(self.faker.date_of_birth(None, 18, 90)) #gives a date birth for the user: we assume they're all adults
		self._user["poivisited"] = []
		self._user["destination"] = None
		self._user["categoriesselected"] = None
		User.idSpark += 1

	def get_attributes(self):
		return self._user

	def get_destination(self):
		return self._user["destination"]

	def get_categories(self):
		return self._user["categoriesselected"]

	def get_id(self):
		return self._user["ID"]

	def get_spark_id(self):
		return self._user["sparkID"]

	def get_poi_visited(self):
		return self._user["poivisited"]


	def simulate(self, destinations, categories):
		"""
		This method is meant to be executed while simulating the initial 
		baseline of Users. It assigns a random POI and preference categories
		from the destination arg and store the resulting relation in the 
		corresponding table.
		Args:
			desitinations: (list) of available POI to be randomly assigned
			categories: (list) of trip categories selected
		""" 

		"""
		Indeed while simulating we are sure that no new POI is added in the current procedure
		Instead when a new user is on the verge of being processed we should be sure to 
		retrieve all possible updated categories from SQL 
		"""
		if not self.baseline:
			raise Exception("No more in the simulation stage!")
		self._user["destination"] = self.faker.random_element(elements=list(destinations))
		self._user['categoriesselected'] = list(self.faker.random_elements(elements=categories, unique=True))


	def simulate_from_database(self):
		"""
		This method generates a random response by tacking into account the unique 
		values stored in the dataset.
		"""
		# retrieval and assignment of POI place (destination)
		sql_dest = '''SELECT DISTINCT mun_name FROM MUNICIPALITIES''';
		destinations = self.postgres_connector.execute(sql_dest)
		destinations_clean = [el[0] for el in destinations]
		self._user["destination"] = self.faker.random_element(elements=list(destinations_clean))

		# retrievial and assignment of POI type (trip category)
		sql_cat = '''SELECT DISTINCT poi_type FROM POI''';
		categories = self.postgres_connector.execute(sql_cat)
		categories_clean = [el[0] for el in categories]
		self._user["categoriesselected"] = list(self.faker.random_elements(elements=categories_clean, unique=True))


	def store_poi_visited_redis(self, poi_spark_id_visited):
		""" 
		Can either update or create a new association:
		Store into Redis the association of user_spark_id : poi_spark_id 
		Args:
			poi_spark_id_visited: spark id of the current POI to be stored
		"""
		access_key = f"{self.get_spark_id()}:visited"
		self.redis_connector.store(access_key, poi_spark_id_visited)


	def retrieve_poi_visited_redis(self, nItems=10000):
		""" 
		Return the list of POI visited already converted in list of integers
		"""
		access_key = f"{self.get_spark_id()}"  # ??? check it out with id:poi as key???
		tmp = self.redis_connector.retrieve(access_key, 0, nItems)
		if type(tmp[0])==str:
			tmp = [int(tmp[i]) for i,_ in enumerate(tmp)]
		return tmp


	def store_preferences_attributes_redis(self, possible_categories):
		"""
		Store in Redis the current user_spark_id : [preferences]
		Args:
			possible_categories: (list) of all possible categories currently available
		"""
		access_key = f"{self.get_spark_id()}:preferences"

		for i,_ in enumerate(possible_categories):
			curr_category = 1 if possible_categories[i] in \
				self.get_attributes()["categoriesselected"] else 0
			self.redis_connector.store(access_key, curr_category)


	def update_preferences_attributes_redis(self, possible_categories):
		"""
		Delete and re store the new set of preferences
		Args:	
			possible_categories: (list) of all categories involved
		"""
		access_key = f"{self.get_spark_id()}:preferences"
		self.redis_connector.delete(access_key)
		self._user['categoriesselected'] = list(self.faker.random_elements(elements=possible_categories, unique=True))
		self.store_preferences_attributes_redis(possible_categories)


	def retrieve_preferences_attributes_redis(self, nItems=10000):
		""" 
		Return the list of preferences visited already converted in list of integers
		"""
		access_key = f"{self.get_spark_id()}:preferences"  
		
		tmp = self.redis_connector.retrieve(access_key)
		if type(tmp[0])==str:
			tmp = [int(tmp[i]) for i,_ in enumerate(tmp)]
		return tmp


	def store_rating_postgres(self, suggestion, rating):
		"""
		We need to retrieve the poi id from their respective databases
		Args:
			suggestion: (str) of poi_name 
			rating: (int) review given [1-5]
		"""
		sql1 = '''SELECT poi_spark_id FROM POI WHERE poi_name=%s''';
		data = (suggestion,)
		tmp_id = self.postgres_connector.execute(sql1, data)
		tmp_id_clean = tmp_id[0][0]
		res = (self.get_spark_id(), tmp_id_clean, rating)
		# storing POI visited
		self.store_poi_visited_redis(tmp_id_clean)

		sql2 = '''INSERT INTO RATINGS(user_spark_id, poi_spark_id, rating) VALUES{};'''.format(res)
		self.postgres_connector.execute(sql2, data)


	def store_profile_attributes_postgres(self):
		"""
		Store the user records in the postgres relation
		"""
		user = self.get_attributes()
		tmp = (user["sparkID"], user["ID"], user["name"], user["surname"], user["sex"], user["dateofbirth"])
		sql = '''INSERT INTO USERS(user_spark_id, user_id, user_name, user_surname, user_sex, user_birth) VALUES{};'''.format(tmp)
		self.postgres_connector.execute(sql)
		

	def update_storages_single(self, categories:list, destination:str, rating:int):
		"""
		Update the Rating relation by a single destination/rating pair.
		Args:
			categories: (list) of current preferences
			destinations: (str) of the POI name
			rating: (int)
		"""
		self.simulate_from_database()
		self.store_profile_attributes_postgres()
		self.store_preferences_attributes_redis(possible_categories=categories)
		self.store_rating_postgres(suggestion=destination, rating=rating)


	def update_storages(self, categories:list, destinations:list, ratings:list):
		"""
		Final user wrapper to store all attributes into the corresponding storages
		allowing to store multiple user destination/rating pairs.
		Args:
			categories: (list) of current preferences 
			destinations: (list) of POI name (string, spark POI ids) 
				suggested and visited (initially at random)
			ratings: (list) ratings (integers) of the corresponding visited POI
		"""
		self.simulate_from_database()
		self.store_profile_attributes_postgres()
		self.store_preferences_attributes_redis(possible_categories=categories)
		assert len(destinations) == len(ratings), "Dimension mismatch between destinations and ratings!"
		for i,_ in enumerate(destinations):
			self.store_rating_postgres(suggestion=destinations[i], rating=ratings[i])



class OldUser():
	""" 
	Sub-optimal implementation of the class which is expected
	to handle an old user coming back and looking for new suggestions
	"""
	def __init__(self, postgres_connector, redis_connector, user_spark_id, 
		destination, possible_categories):

		# connectors
		self.postgres_connector = postgres_connector
		self.redis_connector = redis_connector

		# storing attributes
		self.faker = Faker("it_IT")
		self.possible_categories = possible_categories
		self.user_binary_categories = self.redis_connector.retrieve(f"{user_spark_id}:preferences")
		self._user = dict()
		self._user["sparkID"] = user_spark_id
		self._user["destination"] = destination
		self._user["categoriesselected"] = []

		for i, item in enumerate(self.user_binary_categories):
			if item == "1":
				self._user["categoriesselected"].append(self.possible_categories[i])

		
	def get_attributes(self):
		return self._user

	def get_spark_id(self):
		return self._user["sparkID"]

	def get_destination(self):
		return self._user["destination"]

	def get_categories(self):
		return self._user["categoriesselected"]


	def store_poi_visited_redis(self, poi_spark_id_visited):
		""" 
		Can either update or create a new association:
		Store into Redis the association of user_spark_id : poi_spark_id 
		Args:
			poi_spark_id_visited: spark id of the current POI to be stored
		"""
		access_key = f"{self.get_spark_id()}:visited"  # ??? check it out with id:poi as key???
		self.redis_connector.store(access_key, poi_spark_id_visited)

	def store_preferences_attributes_redis(self, possible_categories):
		"""
		Store in Redis the current user_spark_id : [preferences]
		Args:
			possible_categories: (list) of all possible categories currently available
		"""
		access_key = f"{self.get_spark_id()}:preferences"

		for i,_ in enumerate(possible_categories):
			curr_category = 1 if possible_categories[i] in \
				self.get_attributes()["categoriesselected"] else 0
			self.redis_connector.store(access_key, curr_category)

	def update_preferences_attributes_redis(self, possible_categories):
		"""
		Delete and re store the new set of preferences
		Args:
			possible_categories: (list) of all categories involved
		"""
		access_key = f"{self.get_spark_id()}:preferences"
		self.redis_connector.delete(access_key)
		self._user['categoriesselected'] = list(self.faker.random_elements(elements=possible_categories, unique=True))
		self.store_preferences_attributes_redis(possible_categories)

	def store_rating_postgres(self, suggestion, rating):
		"""
		We need to retrieve the poi id from their respective databases
		Args:
			suggestion: (str) of poi_name 
			rating: (int) review given [1-5]
		"""
		sql1 = '''SELECT poi_spark_id FROM POI WHERE poi_name=%s''';
		data = (suggestion,)
		tmp_id = self.postgres_connector.execute(sql1, data)
		tmp_id_clean = tmp_id[0][0]
		res = (self.get_spark_id(), tmp_id_clean, rating)
		# storing POI visited
		self.store_poi_visited_redis(tmp_id_clean)
		sql2 = '''INSERT INTO RATINGS(user_spark_id, poi_spark_id, rating) VALUES{};'''.format(res)
		self.postgres_connector.execute(sql2, data)
	
	
