build:
	docker build -t kafka_to_feature_store .
run: build
	docker run -it --network=redpanda_network kafka_to_feature_store