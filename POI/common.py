import re
import json
import psycopg2
from termcolor import colored
from run.utils import printBold, printNorm

class POIparser():

	"""
	This class is supposed to connect, extract, filter unstructured data from the 
	Cassandra storage in order to store the resulting filtered POI into a relational table
	"""

	idSpark = 0

	def __init__(self, postgres_connector, cassandra_connector=None):

		self.postgres_connector = postgres_connector
		self.cassandra_connector = cassandra_connector
		self._filepath = None
		if self.cassandra_connector == None:
			printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])	
			printNorm("connection with Cassandra cluster is currently disable", "white", space=0, toend="\n")
			printNorm(".........: proceeding with offline parsing ...", "white", space=0, toend="\n")


	def _retrieve_from_cassandra(self, cql:str, block_name:tuple):
		"""
		Wrapper function to be used to filter the current Cassandra keyspace
		Args:
			cql: (str) 
			block_name: (tuple) name of the Overpass query category to be look up in
				the dataset. (must be passed as tuple in the execution call)
		Return
			cassandra.cluster.ResultSet (iterator object which allows to access to each row)
		"""
		return self.cassandra_connector.retrieve_single_block(cql, block_name)

	def _retrieve_from_json(self):
		pass

	def _clean_string(self, toclean):
		if not isinstance(toclean, str):
			return 
		return toclean.replace("'", " ").replace("*", "").lower().rstrip()

	def _filter(self, currentTags):
		"""
		This helper function takes a single Row from cassandra in order to check the Tags field:
		Args: 
			currentTags: dictionary like cassandra object 
		Return: 
			filterd dictionary (empty if no name is found) 
		"""
		re, tmp = dict(), dict()
		if not "name" in currentTags:
			return re
		for tag in ("addr:city", "addr:street", "addr:housenumber", "addr:postcode", "addr:country"):
			tmp[tag] = currentTags[tag] if tag in currentTags else None
		
		re["name"]      = self._clean_string(currentTags["name"])
		re["poi_place"] = self._clean_string(tmp["addr:city"])
		addr = [tmp["addr:street"], tmp["addr:housenumber"], tmp["addr:postcode"], tmp["addr:country"]]
		while None in addr:
			addr.remove(None)
		re["poi_addr"] = ', '.join(addr) if addr != [] else None
		re["poi_addr"] = self._clean_string(re["poi_addr"])
		return re


	def store_into_postgres_single_from_json(self, jsonfile, verbose=True):
		"""
		Alternative to the store_into_postgres_single (by reqtrieving from Cassandra's Cluster)
		Args:
			jsonfile: (str) path to the json file to be parsed in order to be put
				in a valid postgres relation.
		"""
		block_name = jsonfile.split('.json')[0].split('/')[-1]

		if verbose:
			printBold("[Parsing]: ", "green", space=0, toend="")	
			printNorm(f"now parsing and storing records belonging to block: {block_name}", "white", space=0, toend="\n")

		try:
			tmp = json.load(open(jsonfile))["elements"]
		except:
			raise Exception("No elements found in the current json file!")


		for row in tmp:
			# filtering
			tags_filtered = self._filter(row["tags"])

			# if no name is found, drop the row
			if tags_filtered == dict():
				continue

			values = (
				POIparser.idSpark,
				int(row["id"]),
				float(row["lat"]),
				float(row["lon"]),
				str(block_name),
				tags_filtered["name"],
				tags_filtered["poi_place"],
				tags_filtered["poi_addr"]
				)

			
			# storing POI into postgres Table
			sql='''INSERT INTO PUBLIC.POI(
				poi_spark_id, poi_id, lat, lon, poi_type, poi_name, poi_place, poi_addr
				) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''';
			self.postgres_connector.execute(sql, values)
			
			POIparser.idSpark += 1


	def store_into_postgres_single(self, cql, block_name, verbose=True):
		"""
		Store in postgress the processed Cassandra's row gathered form a single Block

		Args:
			cql: (str) to be passed into the process
			block_name: (str) current block of Overpass Query to be executed 
			verbose: (bool), whether to print intermediate results or not
		Example Usage of a prepared statement
		cql = \"""SELECT tags FROM bdt.rawpoi WHERE block_name = ? ALLOW FILTERING;\"""
		"""
		if verbose:
			printBold("[Parsing]: ", "green", space=0, toend="")	
			printNorm(f"now parsing records belonging to block: {block_name}", "white", space=0, toend="\n")

		tmp = self._retrieve_from_cassandra(cql, block_name)

		for row in tmp:
			# filtering
			tags_filtered = self._filter(row.tags)

			# if no name is found, drop the row
			if tags_filtered == dict():
				continue

			# defining values to be stored
			values = (
				POIparser.idSpark,
				int(row.id),
				float(row.lat.to_eng_string()),
				float(row.lon.to_eng_string()),
				str(row.block_name),
				tags_filtered["name"],
				tags_filtered["poi_place"],
				tags_filtered["poi_addr"]
				)

			if verbose:
				printBold("[Parsing]: ", "green", space=0, toend="")	
				printNorm(f"storing: {block_name} POI block into postgress", "white", space=0, toend="\n")

			# storing POI into postgres Table
			sql='''INSERT INTO PUBLIC.POI(
				poi_spark_id, poi_id, lat, lon, poi_type, poi_name, poi_place, poi_addr
				) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''';
			self.postgres_connector.execute(sql, values)
			
			POIparser.idSpark += 1


	def store_into_postgres_all(self, cql, allBlocks, mode:str="cassandra", verbose=True):
		"""
		cql: (str) cassandra cql sequence to be run
		allBlocks: (list of strings) of blocks's names to be parsed
		mode: (str) from where to get the valid POI to store them in postgres
		"""
		assert mode in ("cassandra", "json"), "Not valid source to parse POI from!"
		raise NotImplementedError
		for block in allBlocks:
			self.store_into_postgres_single(self, cql, block)
 











	### --------------------------------------------------------------------------------
	### deprecated  !!!
	### --------------------------------------------------------------------------------
	
	#self.accepted_categories ={"skiing": ["impiantosci", "piste da sci", "impiantosciistico"],
	#	"ice skating": ["stadioghiaccio", "snowpark","Stadio del ghiaccio"],
	#	"art": ["teatro/opera/cinema", "teatro","Mostra"],
	#	"museums": ["sitoarcheologico", "monumento","museo","luogoculto"],
	#	"camping": ["camping","campeggio"],
	#	"hiking": ["Rifugio/Malga", "rifugio"],
	#	"adventure": ["parco divertimenti/parco a tema"],
	#	"wellness": ["centrowellness","centro benessere", "wellness"],
	#	"gastronomy": ["agriturismo","Caseificio","Prodotto enogatronomico","prodottitipici"]}
		

	def get_current_filepath(self):
		"""
		Return the current filepath of the json that needs to be parsed
		"""
		return self._filepath

	def __parse_dataset(self, filepath_to_json, verbose=True):
		"""
		Parsing the original POI .json file in order to collect valid POI
		Parsed POI are stored in a relational table.
		"""
		complete_ids_list = set()
		used_ids_list = set()
		tmp = self._load_dataset(filepath=filepath_to_json)

		for item in tmp:
			complete_ids_list.add(item['domainId'])
			new_poi = self._clean_poi_extraction(item, self.accepted_categories)
			
			if new_poi == None:
				continue

			used_ids_list.add(new_poi[0])

			# connection establishment: storing POI
			final = (POIparser.idSpark,) + new_poi
			sql='''INSERT INTO PUBLIC.POI(poi_spark_id, poi_id, poi_name, poi_place , poi_type) VALUES (%s,%s,%s,%s,%s)''';
			self.postgres_connector.execute(sql, final)
			POIparser.idSpark += 1
			if verbose:
				print(f"The POI {final} has been added to the POI dataset")
		if verbose:
			print(f"Currently accepted POI: {len(used_ids_list)}")


	def __load_dataset(self, filepath):
		"""
		Temporary loading the dataset into memory
		Args:
			filepath: (str) path to folder to the current json
		Return: 
			dataset as dictionary ( to be parsed )
		"""
		self._filepath = filepath
		with open(self._filepath, encoding='UTF-8') as json_file:
			original_dataset = json.load(json_file)
		return original_dataset

	def __clean_poi_extraction(self, current_structure, filters):
		""" 
		Parsing objects to get the desired structure
		"""
		for i in filters:
			if not current_structure['content']['objData']['category'] in filters[i]:
				continue
			res = list()
			res.append(current_structure['domainId'])
			name = current_structure['content']['objData']['name']['IT']
			try:
				name_clean = name.replace("'", " ").replace('*', '').lower().rstrip()
				name_clean = re.sub(r'[^\x00-\x7F]+', r'', name_clean)
				res.append(name_clean)
			except AttributeError:
				pass
			city = current_structure['content']['poiData']['location']['addresses']['IT']['city']
			try:
				city_clean = city.replace("'", "\'").replace("'", "").lower().rstrip()
				res.append(city_clean)
			except AttributeError:
				pass
			# we store id, name, location and category type for the current poi
			res.append(i)
			# this is a cleaning step to eliminate all the facilities in which 
			# at least one information is missed
			if len(res) == 4:
				if '' not in res:
					return tuple(res)