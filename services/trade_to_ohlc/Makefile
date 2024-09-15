run-dev:
	set KAFKA_BROKER_ADDRESS=localhost:19092&&\
	poetry run python src/main.py

build:
	docker build -t trade_to_ohlc .
run: build
	docker run -it --network=redpanda_network trade_to_ohlc