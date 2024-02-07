from cassandra.cluster import Cluster
from run.utils import printBold, printNorm


class CassandraConnector():

	""" 
	Cassandra connector 
	Warning: you must activate Cassandra from shell first 
	"""

	def __init__(self, contact_points:list=['0.0.0.0'], port:str="9042",
		keyspace_name:str="bdt", table_name:str="rawpoi", initialize:bool=True):

		"""
		!!! It assumes that the keyspace alredy exist !!!
		Initialize the self.cassandra_connector object which can further be called in the 
		self.execute() method
		"""
		self._contact_points = contact_points
		self._port = port
		self._keyspace_name = keyspace_name
		self._table_name = table_name
		self.cassandra_connector = None

		try:
			self.conn = Cluster(contact_points=self._contact_points,port=self._port)
		except:
			raise Exception("Cassandra: Connection not available!")

		if initialize:
			self.cassandra_connector = self.conn.connect()
			self.cassandra_connector.execute(
				"""CREATE KEYSPACE bdt WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"""
				)
			self.cassandra_connector.execute(
				"""CREATE TABLE bdt.rawpoi (id bigint,lat decimal,lon decimal,block_name text,tags map<text, text>,PRIMARY KEY ((id, block_name)));"""
				)
		else:
			self.cassandra_connector = self.conn.connect(self._keyspace_name)

		printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
		printNorm(" Cassandra connection initialized", "white", space=0, toend="\n")


	def get_contact_points(self):
		return self._contact_points

	def get_keyspace_name(self):
		return self._keyspace_name

	def get_port(self):
		return self._port

	def get_table_name(self):
		return self._table_name
		

	def execute(self, cql):
		"""
		General wrapper function to execute any cql query directly on Cassandra
		"""
		self.cassandra_connector.execute(cql)


	def _parse_input(self, tags):
		"""
		Modify 
		"""
		tmp = dict()
		for k in tags:
			tmp[k] = tags[k].replace("'", " ") if len(tags[k].split("'")) > 0 else tags[k]
		return tmp


	def storeJSON(self, JSONobj, block_name, tags):
		"""
		Execute a SQL query and return the result
		Args:
			JSONobj: (dict) must be a valid Json object with fields {'id', 'lat', 'long', 'tags'}
			block_name: (str) name of the corresponding query block name performed over
				the Overpass API
			tags: (dict) current POI tags dictionary 
		"""	
		tags_clean = self._parse_input(tags)
		values = (JSONobj["id"], JSONobj["lat"], JSONobj["lon"], block_name, tags_clean)
		cql = """INSERT INTO bdt.rawpoi (id,lat,lon,block_name,tags) VALUES {};""".format(values)
		self.cassandra_connector.execute(cql)
	

	def retrieve_single_block(self, cql, block_name):
		"""
		Usage: a valid cql needs to be passed into the function with a pointer "?"
		Args:
			cql: (str) 
			block_name: (tuple) name of the Overpass query category to be look up in
				the dataset. (must be passed as tuple in the execution call)
		Return:
			cassandra.cluster.ResultSet (iterator object which allows to access to each row)
		"""

		if not type(block_name) == tuple:
			block_name = tuple([block_name])
		prepared_statement = self.cassandra_connector.prepare(cql)
		return self.cassandra_connector.execute(prepared_statement, block_name)


