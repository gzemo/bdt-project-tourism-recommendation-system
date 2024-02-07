import csv
import time
import requests
from run.utils import printBold, printNorm

class MuncipalityParser():

    """
    This variables allow us to connect to the Geocoding API to collect the information of the 
    corresponding Latitude and Long
    url_start, url_end = "https://geocode.maps.co/search?city=", "&country=IT"

    This variable allow us to filter the dataset only to the Trentino-Alto-Adige region.
    Whenever you would like to change the region parsed, you should also change this variable.
    control_var = "Trentino-Alto Adige/Südtirol"

    Example Usage:
    >>>    postgres_connector = PostgresConnector()
    >>>    municiparser = MuncipalityParser(postgres_connector, control_var)
    >>>    municiparser.parse_dataset(filepath="./municipalities_of_trentino.txt", verbose=True)
    """

    def __init__(self, postgres_connector,  control_var):
        self.postgres_connector = postgres_connector
        self.control_var = control_var
        self._filepath = None
        self.url_init = "https://geocode.maps.co/search?city="
        self.url_final = "&country=IT"

    def get_current_filepath(self):
        """
        Return the current filepath of the txt file that needs to be parsed
        """
        return self._filepath

    def _load_dataset_names(self, filepath):
        """
        Temporary loading the dataset into memory
        Args:
            filepath: (str) path to folder to the current txt
        Return:
            dataset as list of dictionaries ( to be parsed )
        """
        self._filepath = filepath
        with open(self._filepath, encoding='UTF-8') as file:
            line_list = file.readlines()
        return line_list

    def _features_extraction(self, name, timeLimit=0.8, verbose=True):
        """
        Perform the Nominatim (revesed) API request to get back coordinates
        by a given municipality name.
        Args:
            name: (str)
            timeLimit: (float) time to be waited before running a new query
        Return: 
            resulting lat, lon tuple
        """
        clean_name = name.rstrip("\n")
        query_name = clean_name.replace(" ", "+")

        if verbose:
            printBold("[Request]: ", "green", space=3, toend="", attrs=["bold", "dark"])
            printNorm("asking coordinates of: ", "white", space=0, toend="")
            printNorm(f"{clean_name}", "magenta", space=0, toend="\n")

        # we connect to geocode API a retrieve all the informations
        res = requests.request("GET", self.url_init+query_name+self.url_final)
        # check that everything is okay
        if res.status_code != 200:
            print(f"* Response code: {res.status_code}")
            print(f"* Total elapsed: {res.elapsed.total_seconds()}")
            print(f"* Response headers: {res.headers['Date']}")
        clean_res = res.json()
        time.sleep(timeLimit)
        for el in clean_res:
            # avoid storing cities with the same name but in other regions
            if self.control_var in el["display_name"]:

                result = (float(el["lat"]), float(el["lon"]))
                return result

    def parse_dataset(self, filepath, verbose=True):
        """
        Parsing the original municipalities.txt file in order to collect 
        valid latitude and longitude for each municipality of Trentino-Alto-Adige
        Parsed municipalities are stored in a relational table on postgres.
        Args:
            filepath: (str) file path to the txt file in which the list of
                municipalities is stored
        """
        municipalities_list = set()
        tmp = self._load_dataset_names(filepath=filepath)

        for el in tmp:

            coord = self._features_extraction(el)
            clean_el = el.lower().rstrip("\n")

            # connection establishment: storing POI
            municipalities_list.add(el)
            final = (clean_el,) + coord
            sql = '''INSERT INTO PUBLIC.MUNICIPALITIES(mun_name, mun_lat, mun_lon) VALUES (%s,%s,%s)''';
            self.postgres_connector.execute(sql, final)
            
            if verbose:
                printBold("[Storage]: ", "green", space=3, toend="", attrs=["bold", "dark"])
                printNorm("storing coordinates of: ", "white", space=0, toend="")
                printNorm(f"{el}", "magenta", space=0, toend="\n")

        print(f"Currently accepted municipality: {len(municipalities_list)}") if verbose else None


    def parse_dataset_from_file(self, filepath, verbose=True):
        """
        Parsing a csv file of mun_name, lat, lon in order to fill the relational table
        """
        sql = '''INSERT INTO PUBLIC.MUNICIPALITIES(mun_name, mun_lat, mun_lon) VALUES (%s,%s,%s)''';
        with open("POI/municipalities.csv", "r") as f:
            reader = csv.reader(f)
            for i, line in enumerate(reader):
                if i == 0:
                    continue
                final = (line[0], float(line[1]), float(line[2]))
                self.postgres_connector.execute(sql, final)

                if verbose:
                    printBold("[Storage]: ", "green", space=3, toend="", attrs=["bold", "dark"])
                    printNorm("storing coordinates of: ", "white", space=0, toend="")
                    printNorm(f"{line[0]}", "magenta", space=0, toend="\n")
                    
