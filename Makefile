
# start Temporal dev server for local development & testing
# UI is accessible at http://localhost:8233
start-dev-server:
	@echo "starting temporal dev server for testing"
	docker run --rm -p 7233:7233 -p 8233:8233 temporalio/temporal server start-dev --ip 0.0.0.0

# start mongo cluster
mongo:
	@echo "Starting MongoDB cluster..."
	scripts/start-mongo.sh

# setup docker network for local development
network:
	@echo "Creating schenet network if it doesn't exist..."
	@if ! docker network inspect schenet > /dev/null 2>&1; then \
		docker network create schenet; \
		echo "Network schenet created."; \
	else \
		echo "Network schenet already exists."; \
	fi

# start mongo cluster with network check
start-mongo: network mongo

# stop mongo cluster
stop-mongo:
	@echo "Stopping MongoDB cluster..."
	@set -a; . clients/mongodb/mongo.env; set +a; docker-compose -f clients/mongodb/docker-compose-mongo.yml down -v

