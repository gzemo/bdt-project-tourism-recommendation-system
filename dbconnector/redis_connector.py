import os
import time
import redis
from termcolor import colored
from run.utils import printBold, printNorm

class RedisConnector():

	def __init__(self, hostname:str="localhost", port:int=6379):
		try:
			self.conn = redis.Redis(host=hostname, port=port, decode_responses=True)
			printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
			printNorm(" Redis connection initialized", "white", space=0, toend="\n")
		except:
			raise Exception("Redis: Connection not available!")

	def scan_keys(self, pattern):
		"""
		return an iterator of the keys currently stored according to a pattern
		Args:
			pattern: (str), matching pattern to be returned
		Return:
			iterator obj
		"""
		return self.conn.scan_iter(pattern)

	def clear_current_workspace(self, verbose=True):
		"""
		Clear all past Redis history
		"""
		if verbose:
			printBold("[Storage]: ", "green", space=0, toend="", attrs=["bold", "dark"])
			printNorm("WARNING: current Redis storage will be deleted in 3sec !", "red", space=0, toend="\n")

		time.sleep(3)
		#os.system("redis-cli flushdb")
		self.conn.flushdb()

	def store(self, key, value, verbose=False):
		"""
		General purpose key:value storing
		Return:
			None
		"""
		if verbose:
			print(f'Now pushing:  {key} : {value}')
		self.conn.rpush(key, value)

	def retrieve(self, key, nItems=10000):
		"""
		General purpose key retrieval
		Return:
			resulting array
		"""
		return self.conn.lrange(key, 0, nItems)

	def delete(self, key):
		"""
		Remove a key from Redis Storage
		"""
		self.conn.delete(key)