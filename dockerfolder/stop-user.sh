#!/bin/bash

# Colors
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN}Stopping simulation${NC}"

docker compose -f ./docker_compose/docker-compose-postgres.yml stop 
docker compose -f ./docker_compose/docker-compose-redis.yml stop 
docker compose -f ./docker_compose/docker-compose-spark.yml stop 
#sudo docker compose -f ./docker_compose/docker-compose-kafka.yml stop

docker compose -f ./docker_compose/docker-compose-postgres.yml down
docker compose -f ./docker_compose/docker-compose-redis.yml down
docker compose -f ./docker_compose/docker-compose-spark.yml down
#sudo docker compose -f ./docker_compose/docker-compose-kafka.yml down

echo -e "${GREEN}Done! ${NC}"