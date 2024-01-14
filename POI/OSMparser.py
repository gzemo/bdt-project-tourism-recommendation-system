import os
import time
import json
import requests
from termcolor import colored
from run.utils import printBold, printNorm

class OSMparser():
	"""
	Description:
		Open Street Map (OSM) Parser:
		This class defines an instance which is supposed to extract and process information
		gathered from OpenStreetMap  API through Overpass API and Geocode.Maps API available at:
		(Overpass API): http://overpass-api.de/api/interpreter
		(Geocode): https://geocode.maps.com/

	Usage:
		by providing a valid .txt file in which Overpass queries are stored separated by an
		empty line, like:

	Run withouth cassandra connection:
		if you do not want to run and to be connected with Cassandra cluster
		set:
			cassandra_connector = None

	Example:
		This is an example of the syntax used to query from Overpass API (asking to OpenStreetMapAPI)
		[out:json]; //output file type
		// comment
		(
		node["tourism"~"gallery|exihibition_centre|theater"](around:radius,lat,long;
		node["amenity"~"cafe|bar"](around:radius,lat,long);
		);
		out center;
	"""

	def __init__(self, postgres_connector, redis_connector, 
		spark_connector, cassandra_connector, blocksFileName):
		"""
		Args:
			blockFileName: (str) of the path to the block txt file in which 
			each block's query is defined
		"""
		self.postgres_connector = postgres_connector
		self.redis_connector = redis_connector
		self.spark_connector = spark_connector
		self.cassandra_connector = cassandra_connector
		if self.cassandra_connector == None:
			printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])	
			printNorm("connection with Cassandra cluster is currently disable", "white", space=0, toend="\n")
			printNorm(".........: proceeding with offline parsing ...", "white", space=0, toend="\n")

		self.overpass_url = "http://overpass-api.de/api/interpreter"
		self.geocode_maps = "https://geocode.maps.co/reverse?" # add lat={}&long={}
		self.latitue, self.longitude, self.radius = None, None, None

		# Blocks od Overpass query parsing.
		self.blocksFileName = blocksFileName
		self.blocks, self.blocks_name, tmp = [], [], []
		with open(self.blocksFileName, "r", encoding="utf-8") as f:
			lines = f.readlines()
			for line in lines:
				line = line.strip()
				if line != "\n" and not line.startswith("#") and not line.startswith("//"):
					tmp.append(line)
				if line.startswith("//"):
					name = line[3:].replace(" ", "_")
					self.blocks_name.append(name)
				if line == "out center;":
					self.blocks.append(''.join(tmp))
					tmp = []

		assert len(self.blocks_name)==len(self.blocks), "Length mismatch between blocks and names!"


	def _check_result(self, POI):
		"""
		This function perform a check in order to eventually cope with corrupted results
		Args:
			POI: (dict) of a single node gathered from Overpass API
		"""
		check = "type" in POI and "id" in POI and "lat" in POI and "lon" in POI
		if not check:
			raise Exception("Current Node is corrupted!")

	def _extract_tags(self, valid_JSONobj):
		"""
		This function extracts and collapse tags of the given query 
		Args:
			valid_JSONobj: (dict)
		Return:
			tags dictionary obj
		"""
		try:
			tags = valid_JSONobj["tags"]
			if tags == dict():
				raise Exception("Tags field found but Empty!")
		except KeyError:
			raise Exception("No tags field found for the current POI node object!")
		return tags

	def _query_by_block(self, block, timeLimit=0.6, verbose=True):
		""" 
		Manually execute a block.
		Execute the "block" Overpass API query to get back the json corresponding to all POI
		that have been found. A timeLimit variable is initialize in order ot avoid an overflow
		of server request.

		Args:
			block: (str) of Overpass API to be executed
			timeLimit: (float) seconds to be waited before executing the query (default 0.6sec) 
		Return: json of the resulting POI nodes found according to the given query
		"""
		time.sleep(timeLimit)
		response = requests.get(self.overpass_url, params={"data":block})
		if response.status_code != 200:
			print(colored(f"*** Response code: {response.status_code}", "red"))
			print(f"*** Total elapsed: {response.elapsed.total_seconds()}")
			print(f"*** Response headers: {response.headers['Date']}")
			raise ValueError("Server can not handle the request!")	
		data = response.json()
		if verbose:
			print(colored(f"N of elements gathered: {len(data['elements'])}", "green"))
		return data

	def _store_locally(self, JSONobj, blockName, outputdir="./POI/OpenStreetMapTmpStorage"):
		"""
		Store the result from the request done locally in a folder
		Args:
			JSONobj: (dict) the results to be stored as json file 
			blockName: (str) the current block name
			outputdir: (str) output directory.
		"""
		# check names inconsistencies
		if len(blockName.split(" ")) > 0:
			blockName = blockName.replace(" ", "_")

		fileName = os.path.join(outputdir, blockName+".json")
		with open(fileName, "w") as f:
			json.dump(JSONobj , f, indent=4)

	def _store(self, item, block_name, tags):
		"""
		Storing a readily available JSON object into the current Cassandra Session
		It stores a single node dictionary (single entry)

		Args:
			item: (dict) json object of the whole POI node 
			tags: (dict) sub json object of the node's tags
		"""
		self.cassandra_connector.storeJSON(item, block_name, tags)


	def query_store_single_block(self, block, blockName, timeLimit=0.6,
		localCopy=False, intoCassandra=True, verbose=True):
		"""
		Execute a single block request and perform storaging in Cassandra:
		Args:
			block: (str) single Overpass API query to be performed
			blockName: (str) name that identify the current block's category
			timeLimit: (float) time to be waited from one request to the other
			localCopy: (bool) whether to store intermediate json file (default: False)
			intoCassandra: (bool) whether to store into cassandra (default: True)
			verbose: (bool) whether to display the process or not (default: True)
		"""
		s = time.time()
		print(f"Now testing the query:\n{block}") if verbose else None
		currResult = self._query_by_block(block, timeLimit)
		e = time.time()
		print(f"Request time elapsed: {round((e-s)/60,2)} min.") if verbose else None
			
		# storing locally
		if localCopy:
			print(f"Backup locally ...") if verbose else None
			self._store_locally(currResult, blockName, outputdir="./POI/OpenStreetMapTmpStorage")

		# storing into Cassandra
		if intoCassandra:
			print(f"Now storing in Cassandra cluster ...") if verbose else None
			elements = currResult["elements"]
			for item in elements:
				self._check_result(item)
				tags = self._extract_tags(item)
				self._store(item, blockName, tags)


	def query_store_all_blocks(self, timeLimit=0.6, localCopy=False,
		intoCassandra=True, verbose=True):
		"""
		Execute each block request and perform storaging in Cassandra:
		Args:
			block: (str) single Overpass API query to be performed
			timeLimit: (float) time to be waited from one request to the other
			localCopy: (bool) whether to store intermediate json file (default: False)
			verbose: (bool) whether to display the process or not (default: True)
		"""
		for i,block in enumerate(self.blocks):

			print(f"\nPerforming POI request for category:  {self.blocks_name[i]}")

			self.query_store_single_block(block=block, 
										blockName=self.blocks_name[i], 
										timeLimit=timeLimit,
										localCopy=localCopy, 
										intoCassandra=intoCassandra)


	def store_from_files(self, file):
		"""
		File name must match the corresponding Block_name
		Store in Cassandra by locally stored files
		Args:
			file: valid json file with field "elements"
		"""
		blockName = file.split(".")[0]
		with open(file) as json_data:
			data = json.load(json_data)
			for item in data['elements']:
				self._check_result(item)
				tags = self._extract_tags(item)
				self._store(item, blockName, tags)


"""

### how to run ###

parser = OSMparser(postgres_connector, redis_connector, 
	spark_connector, cassandra_connector, './POI/overpass_query_block.txt')

# tesing a single block
#parser.query_store_single_block(parser.blocks[1], parser.blocks_name[1], 
#	localCopy=True, intoCassandra=True)

# testing all
parser.query_store_all_blocks(timeLimit=2, 
							localCopy=False,
							intoCassandra=True,
							verbose=True)
"""