import sys
from termcolor import colored, cprint

class ComputeStatistics():

    """
    Usage:
        postgres_connector = PostgresConnector()
        current_stats = ComputeStatistics(postgres_connector)
        current_stats.most_visited_poi()
        current_stats.most_appreciated_poi()
        current_stats.most_frequent_user()
        current_stats.most_satisfied_user()
    """

    def  __init__(self, postgres_connector):
        self.postgres_connector = postgres_connector
        cprint("[Simulation]: ", "blue", attrs=["bold"], file=sys.stderr, end="\n")
        cprint("[Simulation]: ", "blue", attrs=["bold"], file=sys.stderr, end="")
        cprint("Performing final Statistics", "blue", file=sys.stderr)

    def most_visited_poi(self):
        """Returns:
        the poi visited the most during the simulation"""
        sql1 = """SELECT poi_spark_id, COUNT (poi_spark_id) as frequency FROM ratings GROUP BY poi_spark_id ORDER BY frequency DESC ;"""
        res1 = self.postgres_connector.execute(sql1)

        poi_most_visited_id = (res1[0][0], )
        poi_most_visited_frequency = res1[0][1]

        sql2 = """SELECT poi_name FROM POI WHERE poi_spark_id=%s;"""
        res2 = self.postgres_connector.execute(sql2, poi_most_visited_id)
        poi_most_visited_name = res2[0][0]

        print (f"   1) The point of interest most visited is {poi_most_visited_name}, with {poi_most_visited_frequency} visits.")
        return str('POI name: '+poi_most_visited_name+'  rating: '+str(poi_most_visited_frequency))
   
    def most_appreciated_poi(self):
        """Returns:
        the poi among the ones visited with the highest scores"""
        sql1 = """SELECT poi_spark_id, SUM(rating) / COUNT(poi_spark_id) as avarage_score FROM ratings GROUP BY poi_spark_id ORDER BY avarage_score DESC;"""
        res1 = self.postgres_connector.execute(sql1)
        poi_most_rated_id = (res1[0][0],)
        poi_most_rated_avarage_rating = res1[0][1]

        sql2 = """SELECT poi_name FROM POI WHERE poi_spark_id=%s;"""
        res2 = self.postgres_connector.execute(sql2, poi_most_rated_id)
        poi_most_rated_name = res2[0][0]
        
        print (f"   2) The point of interest with the highest avarage rating of {poi_most_rated_avarage_rating} is {poi_most_rated_name}.")
        return str('POI name: '+poi_most_rated_name+'  rating: '+str(poi_most_rated_avarage_rating))
    
    def most_frequent_user(self):
        """Returns:
        the user who took the highest number of trips"""
        sql1 = """SELECT user_spark_id, COUNT (user_spark_id) as frequency FROM ratings GROUP BY user_spark_id ORDER BY frequency DESC;"""
        res1 = self.postgres_connector.execute(sql1)
        user_most_frequent_id = (res1[0][0],)
        user_most_frequent_frequency = res1[0][1]

        sql2 = """SELECT user_name, user_surname FROM USERS WHERE user_spark_id=%s;"""
        res2 = self.postgres_connector.execute(sql2, user_most_frequent_id)
        user_most_frequent_name = res2[0][0]
        user_most_frequent_surname = res2[0][1]

        print (f"   3) The user {user_most_frequent_name} {user_most_frequent_surname} took the highest number of trip with our platform, which is {user_most_frequent_frequency} trips.")
        return str(user_most_frequent_name+' '+user_most_frequent_surname)

    def most_satisfied_user(self):
        """Returns:
        the user who gave the higher scores overall"""
        sql1 = """SELECT user_spark_id, SUM(rating) / COUNT(user_spark_id) as avarage_score FROM ratings GROUP BY user_spark_id ORDER BY avarage_score DESC;"""
        res1 = self.postgres_connector.execute(sql1)
        user_most_appreciate_id = (res1[0][0],)

        sql2 = """SELECT user_name, user_surname FROM USERS WHERE user_spark_id=%s;"""
        res2 = self.postgres_connector.execute(sql2, user_most_appreciate_id)
        user_most_appreciate_name = res2[0][0]
        user_most_appreciate_surname = res2[0][1]

        print (f"   4) The user who was most satisfied with the recommended points of interest was {user_most_appreciate_name} {user_most_appreciate_surname}.")
        return str(user_most_appreciate_name+' '+user_most_appreciate_surname)

    def print_all(self):
        self.most_visited_poi()
        self.most_appreciated_poi()
        self.most_frequent_user()
        self.most_satisfied_user()