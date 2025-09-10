
# start Temporal dev server for local development & testing
# UI is accessible at http://localhost:8233
start-dev-server:
	@echo "starting temporal dev server for testing"
	docker run --rm -p 7233:7233 -p 8233:8233 temporalio/temporal server start-dev --ip 0.0.0.0
