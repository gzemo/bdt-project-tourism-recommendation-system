# Tripendum: your next trip in Trentino
![alt text](./logo/nettuno_fountain.png?raw=true)

## Abstract
The project was designed for the Big Data Technologies’ course (a.y. 2022/2023).<br>
The purpose of this project was to implement a recommendation system to suggest tourism-related experiences to potential clients visiting Trentino-Alto-Adige by exploiting real as well as customly-built, synthetically generated data from *Open Street Map* API (https://www.openstreetmap.org/about)

---

## Technologies
Apache Cassandra<br>
Apache Spark<br>
Docker<br>
PostgreSQL<br>
Redis

---

## Functionalities
* [x] Pool of baseline users estimation
* [x] Similarity estimation (by Apache Spark MLib implementation)
* [x] ALS algorithm implementation (by Apache Spark MLib implementation)
* [x] Simulation of new clients' request
* [ ] Cassandra implementation on Docker
* [ ] Municipalities' scraping and parsing implementation 
* [ ] Pub/sub implementation to handle the queue of users requests
* [ ] Enable an easy to use module to include and automatically parse raw OpenStreetMap nodes from other geographical areas
---

## Architecture
Conceptual, logical and physical data model.
![alt text](./simulation_screens/data_models.png?raw=true)
Pipeline of the project.
![alt text](./simulation_screens/pipeline.png?raw=true)

---

## Project Files

### POI folder
- `common.py`: parsing, cleaning and storing process for POIs' raw data
- `municipality_parser.py`: retrieving process for municipalities' data from *Geocoding*’s API
- `OSMparser.py`: retrieving and storing process for POIs' data from *Overpass*’s API 
- `overpass_query_block.txt`: list of *Overpass*’ categories related to tourism
- `municipalities_of_trentino.txt`: list of Trentino-Alto-Adige's municipalities
- `selection_POI.py`: recomendation output handler to filter the final outputs 
- `OpenStreetMapTmpStorage`: set of JSON files containing the POIs retrieved from *Overpass*’ API divided by categories

### clients folder
- `common.py`: list of functions for Classes User and OldUser

### dbconnector folder
Contains all the `connector.py` files necessary to connect to the technologies implemented 

### run folder
- `recommend.py`: implementation of the recommendation engine by ALS algorithm and periodical updates
- `similarity.py`: implementation of the Jaccard's similarity score estimation and maintenance 
- `statistics.py`: final simulation statistics
- `utils.py`: set of functions used inside the simulation.

### docker_compose folder
Contains all the `docker-compose` files necessary for delivering the simulation.<br>
For each technology implemented we defined a single docker compose file which will be then activate via the `start-user.sh` wrapper executor which will, eventually, stop existing process to further activate the corresponding containers.

### main.py 
is the master file which allows the simulation to be executed.

---

## How to Run
The project's demo can be executed using Docker's application.<br>
In this case, the parsing process from *Overpass* and *Geocoding*'s API will be avoided and already-stored data will be used.<br>

Using Docker you will be able to start the technologies implemented all at once and to run them simultaneously in a safe, self-contained environment.<br>



In a new shell enter the folder and run:
```shell
docker build -f ./dockerfile.yaml -t bdtimage:latest . 
```
This is going to create the initial docker image that will further be used to run a containers' network in which the simulation will take place.

The *bdtimage* image is created with the tag latest inside docker (it might take some minutes!).
```shell
docker run --name bigdata -it --network="host" bdtimage:latest 
```
We run a container named *bigdata* using the `bdtimage:latest` image.<br>
We have now interactive access to the docker container.  

In a new local shell you need then to get into the `dockerfolder` folder and run:
```shell
./start-user.sh   # or alternatively with "bash start-user.sh"
```

At this point all the containers initialised in the docker compose will be firstly downloaded and then activated.

From the previous docker shell get into  the `/bdtproject` folder and run:
```shell
python3 main.py
```
To let the initial simulation start.

To stop the simulation and close the existing connection the the loaded docker containers, chance you need to run
```shell
./stop-user.sh   # or alternativerly with "bash stop-user.sh"
```
---

### Simulation 
Before starting the simulation some tunable parameters can be modified in the `main.py` file.

```python
COMPLETE_SIMULATION          = True # run simulation from beginning; otherwise simulate the user request 
PARSING_FROM_CASSANDRA       = False # whether to perform the Overpass API request, store into cassandra and filter to postgress
PARSING_FROM_GEOCODING       = False # whether to retrieve the municipality's coordinates from (reverse) geocoding maps
N_BASELINE_USERS             = 300 # number of baseline user to fill as starting entries
N_SIMULATED_REQUEST          = 15  # number of request receiving at the end of the simulation
PROBABILITY_NEW_USER_COMING  = 0.5 # probability of having a simulating user being a new, unregistered one
N_POI_TO_RETURN              = 100  # number of final POI suggestion to be retained
INITIAL_USER_DISTANCE        = 10 # initial radius from where to look while filtering for the best recommended POI
PAST_USER_ATTEMPTS           = 5 # changing locations attempts while dealing with underepresented trip's categories
NEW_USER_SIMILARITY_TRESHOLD = 0.10 # max similarity threshold
NEW_USER_MIN_N_NEIGHBOURS    = 15 # minimum number of similar user from which take the averaged ratings
INITIAL_USER_DISTANCE        = 10 # initial radius from where to look to filter the recommended POI
MAX_USER_DISTANCE            = 100 # maximum allowed radius to find POIs
NEW_USER_GAP_TO_UPDATE       = 3 # triggering model re-train after how much new user registered
```

---

The current framework has been tested with:
- Ubuntu 22.04 LTS & macOS Ventura 13.31.1
- Python 3.9
- Docker version 24.0.2

---

### Addtional steps: further improvements and how to run without docker
It is also possible to run the entire simulation, including the request tasks to *Overpass*’ API and *Geocoding*'s API.<br>
We suggest to use this simulation if you would like to use another Italian region.<br>
To do so, you should consider the following notes:
1. this simulation will not use Docker, since Cassandra was not implemented to be containarized. 
2. before initiating the simulation you should create a KEYSPACE named bdt in Apache Cassandra from your terminal and keep Cassandra's connection on.
3. change the parameter corresponding to your set of credential in order to connect to your local Postgres, Spark and Redis in the initial stage of the `main.py`

To apply the simulation to another region, please note that you should also update:
- `municipalities_of_trentino.txt` with a new file containing all the municipalities of your desired region. 
- the coordinates to parse to *Overpass*' API: we suggest to use latitude and longitude of the Regional County Seat.
- change the parameters `PARSING_FROM_CASSANDRA` and `PARSING_FROM_GEOCODING` as True from the `main.py` to enable the data storing 


### Credits and third party licences:
- OpenStreetMap Point Of interest are licensed by: Open Data Commons Open Database License (ODbL) (<https://opendatacommons.org/licenses/odbl/>)
- Picture of Nettuno's fountain, Piazza del Duomo, Trento: Zairon, CC BY-SA 4.0 <https://creativecommons.org/licenses/by-sa/4.0>, via Wikimedia Commons
