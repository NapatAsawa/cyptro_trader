run-live-dev:
	set KAFKA_BROKER_ADDRESS=localhost:19092&&\
	set LIVE_OR_HISTORICAL=live&&\
	poetry run python src/main.py

run-hist-dev:
	set KAFKA_BROKER_ADDRESS=localhost:19092&&\
	set LIVE_OR_HISTORICAL=historical&&\
	poetry run python src/main.py
build:
	docker build -t trade_producer .
run: build
	docker run -it --network=redpanda_network trade_producer