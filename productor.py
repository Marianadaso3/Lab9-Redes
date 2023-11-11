from confluent_kafka import Producer
import random
import json
import time
import struct

class KafkaDataProducer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self._create_producer()

    def _create_producer(self):
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'python-producer'
        }
        return Producer(producer_config)

    def encode_data(self, data):
        encoded_temp = int((data['temperatura'] + 50) * 100)
        encoded_hume = int(data['humedad'])
        wind_directions = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
        encoded_wind = wind_directions.index(data['direccion_viento'])
        encoded_data = struct.pack('>HBB', encoded_temp, encoded_hume, encoded_wind)
        return encoded_data

    def send_data(self, key, value):
        self.producer.produce(self.topic, key=key, value=value)
        self.producer.flush()

def generate_random_data():
    temperatura = round(random.gauss(50, 10), 1)
    humedad = round(random.gauss(50, 10), 1)
    direccion = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])
    return {'temperatura': temperatura, 'humedad': humedad, 'direccion_viento': direccion}

def main():
    bootstrap_servers = 'lab9.alumchat.xyz:9092'
    topic = '201055'

    kafka_producer = KafkaDataProducer(bootstrap_servers, topic)

    try:
        while True:
            data = generate_random_data()
            encoded_data = kafka_producer.encode_data(data)
            kafka_producer.send_data(key='sensor201055', value=encoded_data)

            print(f"Datos enviados: {data}")
            time.sleep(3)

    except KeyboardInterrupt:
        print("[!] IMPORTANTE: Interrupción del usuario. Cerrando el productor Kafka.")

if __name__ == "__main__":
    main()
