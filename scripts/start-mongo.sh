#!/bin/bash

# Mongo environment variables
ENV_FILE="./pkg/clients/mongodb/mongo.env"

# Check if the environment file exists and source it
if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
else 
    echo " - ${ENV_FILE} does not exist."
    exit 2
fi

# move to root directory
BASEDIR=$(dirname "$0")
echo " - Base dir: ${BASEDIR}"
cd ${BASEDIR}/../

# Start MongoDB using docker-compose
docker-compose -f pkg/clients/mongodb/docker-compose-mongo.yml up -d