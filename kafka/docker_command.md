### go to producer container
docker exec -it kafka-producer-1 bash


### go to producer container
docker exec -it kafka-consumer-1 bash

### start producer instance
docker exec -d kafka-producer-1 bash -c 'python3 main.py --worker 1 --bootstrap-server $KAFKA_HOST --topic $KAFKA_TOPIC'

### start consumer instance
docker exec kafka-consumer-1 bash -c 'python3 main.py --bootstrap-server $KAFKA_HOST --topic $KAFKA_TOPIC --tablename currencies'
