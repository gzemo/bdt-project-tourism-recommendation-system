from pyspark.sql import SparkSession
from run.utils import printBold, printNorm


class SparkConnector():

	def __init__(self, pathToJarFile:str, masterName:str="local",
		userName:str="postgres", password:str="123", 
		driver:str="org.postgresql.Driver"):
		"""
		For configuration purpose:
		Jar file available at: https://jdbc.postgresql.org/download/
		"""		
		self._masterName = masterName
		self._userName = userName
		self._password = password
		self._pathToJarFile = pathToJarFile
		self._driver = driver

		self.spark_connector = SparkSession \
								.builder \
								.master(self._masterName) \
								.appName("Bdt") \
								.config("spark.jars", self._pathToJarFile) \
								.getOrCreate()	

		printBold("[Processing]: ", "cyan", space=0, toend="")
		printNorm("Spark connection initialized", "white", space=0, toend="\n")

	def get_connector(self):
		return self.spark_connector

	def get_pathToJarFile(self):
		return self._pathToJarFile


	def createDF(self, item, listofCols):
		"""
		item: (list of tuple) to be passed in the createDataFrame
		listofCols: (list) (ex. ["id","features"])
		"""
		return self.spark_connector.createDataFrame(item, listofCols)