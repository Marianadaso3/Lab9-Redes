from confluent_kafka import Producer
import random
import json
import time
import struct

# Configura el servidor y el topic de Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '201055'

# Configura el productor Kafka
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'python-producer'
}

producer = Producer(producer_config)

# aqui vamos a poner la parte nueva de codigo para el ejercicio 3.4 donde codificamos la data
def encode_data(data):
    encoded_temp = int((data['temperatura'] + 50) * 100)  # Rango de temperatura: -50 a 50
    encoded_hume = int(data['humedad'])
    # Codificar la dirección del viento
    wind_directions = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
    encoded_wind = wind_directions.index(data['direccion_viento'])

    # Empaquetar los datos en 3 bytes (24 bits)
    encoded_data = struct.pack('>HBB', encoded_temp, encoded_hume, encoded_wind)

    return encoded_data

# Función para generar datos aleatorios con distribución normal
def generar_data():
    temperatura = round(random.gauss(50, 10), 1)  # Media 50, Varianza 10
    humedad = round(random.gauss(50, 10), 1)  # Media 50, Varianza 10
    direccion = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])
    datos = {'temperatura': temperatura,
             'humedad': humedad, 'direccion_viento': direccion}
    return datos


try:
    while True:
        # Generar datos aleatorios
        data = generar_data()

        # Codificar los datos antes de enviar
        encoded_data = encode_data(data)

        # Enviar los datos al servidor Kafka
        producer.produce(topic, key='sensor201055', value=encoded_data)
        producer.flush()

        print(f"Datos enviados: {data}")
        time.sleep(3)
except KeyboardInterrupt:
    print("[!] IMPORTANTE: Se interrupción del usuario. Cerrando el productor Kafka.")
