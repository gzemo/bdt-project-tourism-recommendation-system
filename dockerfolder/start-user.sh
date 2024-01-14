#!/bin/bash

# Colors
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN}Starting simulation ${NC}"

docker compose -f ./docker_compose/docker-compose-postgres.yml stop
docker compose -f ./docker_compose/docker-compose-redis.yml stop 
docker compose -f ./docker_compose/docker-compose-spark.yml stop
#sudo docker compose -f ./docker_compose/docker-compose-kafka.yml stop 

docker compose -f ./docker_compose/docker-compose-postgres.yml up -d
docker compose -f ./docker_compose/docker-compose-redis.yml up -d
docker compose -f ./docker_compose/docker-compose-spark.yml up -d
#sudo docker compose -f ./docker_compose/docker-compose-kafka.yml up -d

echo -e "${GREEN}Done! ${NC}"

