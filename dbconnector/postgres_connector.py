import psycopg2
from run.utils import printBold, printNorm
#from POI.common import POI


class PostgresConnector():

	def __init__(self, dbname:str="bdt", hostname:str="localhost",
		username:str="postgres", password:str="123", port:str="5432"):

		self._dbname = dbname
		self._hostname = hostname
		self._username = username
		self._password = password
		self._port = port
		try:
			self.conn = psycopg2.connect(database=self._dbname,
										host=self._hostname,
										user=self._username,
										password=self._password,
										port=self._port)
			self.isConnected = False
			self.initialize_cursor()

			printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
			printNorm(" Postgress connection initialized", "white", space=0, toend="\n")
		except:
			raise Exception("PostgresSQL: Connection not available!")

	def get_dbname(self):
		return self._dbname
	def get_hostname(self):
		return self._hostname
	def get_username(self):
		return self._username
	def get_password(self):
		return self._password
	def get_port(self):
		return self._port

	def check_init(self):
		"""
		Flag to be checked while performing query
		"""
		return self.isConnected


	def initialize_cursor(self):
		"""
		Initialize the cursor object
		"""
		self.isConnected = True
		self.conn.autocommit = True
		self.cursor = self.conn.cursor()


	def execute(self, query, arg:tuple=tuple()):
		"""
		Execute a SQL query and return the result
		Args:
			query: (str)
			arg: (tuple)  additional argument
		"""
		re = None
		if not self.check_init():
			raise Exception("PostgresSQL cursor not initialized yet!")
		if len(arg) == 0:
			self.cursor.execute(query)
		else:
			self.cursor.execute(query, arg)
		try:
			re = self.cursor.fetchall()
		except:
			pass
		self.conn.commit() # although not necessary
		return re

	def close(self):
		"""
		Close the current connection
		"""
		if not self.check_init():
			raise Exception("PostgresSQL cursor not initialized yet!")
		self.close()
