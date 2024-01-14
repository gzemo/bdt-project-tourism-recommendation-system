import pandas as pd
from geopy import distance
from run.utils import printBold, printNorm


class SelectionPOI():
    def __init__(self, postgres_connector):
        """
        args:
            current_pois: (list) of all suggested pois after the recommendation
            destination: (str) user's chosen destination
        """
        self.postgres_connector = postgres_connector
        self.current_pois = None
        self.destination = None
        self.destination_coordinates = None
        self.all_top_suggestion_performed = False


    def add_current_pois(self, current_pois):
        self.current_pois = current_pois

    def add_current_destination(self, destination):
        self.destination = destination
        self._convert_destination_into_coordinates()

    def _convert_destination_into_coordinates(self):
        """
        assign to the "destination_coordinates" attribute the actual tuple of coordinates
        args:
            destination: (str) name of the place to look for
        return:
            none
        """
        sql_lat = """select mun_lat from municipalities where mun_name=%s""";
        sql_lon = """select mun_lon from municipalities where mun_name=%s""";
        current_lat = self.postgres_connector.execute(sql_lat, (self.destination,))
        current_lon = self.postgres_connector.execute(sql_lon, (self.destination,))
        self.destination_coordinates = (current_lat[0][0], current_lon[0][0])

    def _compute_distance(self, current_poi):
        """
        args:
            current_poi: is the name of the poi suggested by the recommendation algorithm
        return:
            the distance (in km) between the destination and the poi suggested
        """

        sql_lat = """select lat from poi where poi_name=%s""";
        sql_lon = """select lon from poi where poi_name=%s""";
        current_lat = self.postgres_connector.execute(sql_lat, (current_poi,))
        current_lon = self.postgres_connector.execute(sql_lon, (current_poi,))
        coordinates_poi = (current_lat[0][0], current_lon[0][0])

        dist = distance.distance(self.destination_coordinates, coordinates_poi).km

        return dist

    def _retrieve_category(self, current_poi):
        """
        args:
            current_poi: is the name of the poi suggested by the recommendation algorithm
        return:
            the category of suggested poi retrieved from the poi dataset
        """
        sql = """select poi_type from poi where poi_name=%s""";
        current_cat = self.postgres_connector.execute(sql, (current_poi,))
        cat = current_cat[0][0]

        return cat

    def _build_columns(self):
        """
        return:
            distances: list of all the distances between the pois in self.current_pois and the destination
            categories: list of all the categories of self.current_pois
        """
        distances = []
        categories = []
        if not self.current_pois:
            raise Exception("new user list of top recommended poi is missing!")
        if not self.destination:
            raise Exception("new user destination is missing!")

        for current_poi in self.current_pois:
            dist = self._compute_distance(current_poi)
            cat = self._retrieve_category(current_poi)
            distances.append(dist)
            categories.append(cat)
        self.all_top_suggestion_performed = True

        return distances, categories

    def select_poi(self, df, user_categories, user_distance, toreturn=15):
        """
        resulting filtered poi must match the
        args:
            user_categories: is a list of the activity types preferred by the user
            user_distance: is ray (in km) decided by the user
        return:
            the list of names of suggested poi filtered
        """

        if not self.all_top_suggestion_performed:
            raise Exception("need to compute the distance first!")

        df1 = df[df.distance < user_distance] #filter for distance
        df2 = df1[df1["type"].isin(user_categories)] #filter for category
        df3 = df2.sort_values(by=["distance"]) # order by distance
        clean_pois = df3["name"].tolist()

        return clean_pois


    def convergence_select_poi(self, df, user_categories, user_distance, max_distance, toReturn=15, verbose=True):
        """
        Iteratively enlarge the initial chosen user radius to find any suggested user recommender
        """
        closest_pois = self.select_poi(df, user_categories, user_distance)
        while len(closest_pois) == 0 and user_distance < max_distance:
            printBold("[Filtering]: ", "yellow", space=6, toend="")
            printNorm(f"No POI found around {user_distance} km.", "white", space=0, toend="\n")
            user_distance += 5
            closest_pois = self.select_poi(df, user_categories, user_distance)

        if len(closest_pois) > 0:
            if verbose:
                printBold("[Filtering]: ", "yellow", space=6, toend="")
                printNorm(f"Found {len(closest_pois)} POI within {user_distance} km.", "white", space=0, toend="\n")
                printBold("[Filtering]: ", "yellow", space=6, toend="")
                printNorm(f"Now displaying the top {len(closest_pois)}", "white", space=0, toend="\n")
                printNorm(f"..........: {closest_pois[0:toReturn]}", "cyan", space=6, toend="\n")
                return closest_pois[0:toReturn]

        else:
            if verbose:
                printBold("[Filtering]: ", "yellow", space=6, toend="")
                printNorm(f"No suggestion found within {max_distance} km.", "white", space=0, toend="\n")
            return []


##in main.py


### -----------------------------------------------------------
### deprecated 
### -----------------------------------------------------------

class SelectionPOI_old():
    def __init__(self, postgres_connector):
        """
        Args:
            current_pois: (list) of all suggested pois after the recomendation
            destination: (str) user's chosen destination 
        """
        self.postgres_connector = postgres_connector
        self.current_pois = None
        self.destination = None
        self.destination_coordinates = None
        self.all_top_suggestion_performed = False

    def add_current_pois(self, current_pois):
        self.current_pois = current_pois

    def add_current_destination(self, destination):
        self.destination = destination
        self._convert_destination_into_coordinates()

    def _convert_destination_into_coordinates(self):
        """
        Assign to the "destination_coordinates" attribute the actual tuple of coordinates
        Args:
            destination: (str) name of the place to look for
        Return:
            None
        """
        sql_lat = """SELECT mun_lat FROM MUNICIPALITIES WHERE mun_name=%s""";
        sql_lon = """SELECT mun_lon FROM MUNICIPALITIES WHERE mun_name=%s""";
        current_lat = self.postgres_connector.execute(sql_lat, (self.destination,))
        current_lon = self.postgres_connector.execute(sql_lon, (self.destination,))
        self.destination_coordinates = (current_lat[0][0], current_lon[0][0])

    def _compute_distance(self, current_poi):
        """
        Args:
            current_poi: is the name of the poi suggested by the recommendation algorithm
        Return:
            the distance (in km) between the destination and the poi suggested
        """

        sql_lat = """SELECT lat FROM poi WHERE poi_name=%s""";
        sql_lon = """SELECT lon FROM poi WHERE poi_name=%s""";
        current_lat = self.postgres_connector.execute(sql_lat, (current_poi,))
        current_lon = self.postgres_connector.execute(sql_lon, (current_poi,))
        coordinates_poi = (current_lat[0][0], current_lon[0][0])

        dist = distance.distance(self.destination_coordinates, coordinates_poi).km

        return dist

    def _retrieve_category(self, current_poi):
        """
        Args:
            current_poi: is the name of the poi suggested by the recommendation algorithm
        Return:
            the category of suggested poi retrieved from the POI dataset
        """
        sql = """SELECT poi_type FROM poi WHERE poi_name=%s""";
        current_cat = self.postgres_connector.execute(sql, (current_poi,))
        cat = current_cat[0][0]

        return cat

    def _insert_into_top(self, current_poi, current_category, current_distance):
        """
        Args:
            current_poi: is the name of the poi suggested by the recommendation algorithm
            current_category: is the category type of the current_poi
            current_distance: is the distance (in km) between current_poi and destination
        Return:
            save the details into a temporary dataset
        """
        res = (current_poi, current_category, current_distance)
        #i'm just going yo pretend that it's a fake list that we still need to retrieve
        sql = """INSERT INTO TOP_POI (poi_name, poi_type, poi_distance) VALUES{};""".format(res)
        self.postgres_connector.execute(sql)


    def _perform_and_insert_distance_for_all(self):
        """
        For each suggested POI in the top N recomendation perform the distance with
        respect to the destination choosen and store in the table
        """
        if not self.current_pois:
            raise Exception("New User list of top recommended poi is missing!")
        if not self.destination:
            raise Exception("New User destinatino is is missing!")
        for current_poi in self.current_pois:
            dist = self._compute_distance(current_poi)
            cat  = self._retrieve_category(current_poi)
            self._insert_into_top(current_poi, cat, dist)
        self.all_top_suggestion_performed = True


    def select_poi(self, user_categories, user_distance, toReturn=15):
        """
        Resulting filterd POI must match the 
        Args:
            user_categories: is a list of the activity types preferred by the user
            user_distance: is ray (in km) decided by the user
        Return:
            the list of names of suggested poi filtered
        """

        self._perform_and_insert_distance_for_all()

        if not self.all_top_suggestion_performed:
            raise Exception("Need to compute the distance first!")

        #res = (tuple(user_categories), (user_distance,))
        res = (user_categories, (user_distance,))

        sql = """SELECT poi_name FROM TOP_POI WHERE poi_type = ANY (%s) AND poi_distance < (%s) ORDER BY poi_distance""";
        poi_filtered = self.postgres_connector.execute(sql, res)
        clean_pois = [el[0] for el in poi_filtered]

        # after the extraction is completed we clean the table (we don't drop it!), to be used for the next user
        sql1 = """DELETE FROM TOP_POI;"""
        self.postgres_connector.execute(sql1)

        return clean_pois


    def convergence_select_poi(self, user_categories, user_distance, max_distance, toReturn=15, verbose=True):
        """
        """
        closest_pois = self.select_poi(user_categories, user_distance)
        while len(closest_pois) == 0 and user_distance < max_distance:
            printBold("[Filtering]: ", "yellow", space=6, toend="")
            printNorm(f"No POI found around {user_distance} km.", "white", space=0, toend="\n")            
            user_distance += 5
            closest_pois = self.select_poi(user_categories, user_distance)

        if len(closest_pois) > 0:  
            if verbose:
                printBold("[Filtering]: ", "yellow", space=6, toend="")
                printNorm(f"Found {len(closest_pois)} POI within {user_distance} km.", "white", space=0, toend="\n")          
                printBold("[Filtering]: ", "yellow", space=6, toend="")
                printNorm(f"Now displaying the top {len(closest_pois)}", "white", space=0, toend="\n")          
                printNorm(f"..........: {closest_pois[0:toReturn]}", "cyan", space=6, toend="\n")
                return  closest_pois[0:toReturn]

        else:
            if verbose:
                printBold("[Filtering]: ", "yellow", space=6, toend="")
                printNorm(f"No suggestion found within {max_distance} km.", "white", space=0, toend="\n")
            return []