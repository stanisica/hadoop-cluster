docker compose -f ./docker-compose.yaml up -d
sleep 10

docker exec -it spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ./consumer.py
kafka-console-consumer --zookeeper zookeeper:2181 --topic stream