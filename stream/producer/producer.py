import os
import time
import pandas as pd
from kafka import KafkaProducer
from json import dumps

def main():
    broker_address = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    topic_name = 'stream'  

    producer = KafkaProducer(bootstrap_servers=[broker_address],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    file_path = '/usr/src/app/stream.csv'
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic_name, value=message)
        print(f"Sent message: {message}")
        time.sleep(60)

    producer.close()

if __name__ == "__main__":
    main()