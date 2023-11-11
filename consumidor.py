#Autores: Mariana David y Pablo Escobar
#Redes - 2023
#Laboratorio 9
#Programa: consumidor.py

import threading
import tkinter as tk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from confluent_kafka import Consumer, KafkaError
import json
import struct

class KafkaDataConsumer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = self._create_consumer()
        self.all_temp = []
        self.all_hume = []
        self.all_wind = []
        self.figure = Figure(figsize=(5, 4), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.figure, master=root)
        self.canvas.get_tk_widget().pack()

    def _create_consumer(self):
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'my-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([self.topic])
        return consumer

    def decode_data(self, encoded_data):
        decoded_temp, decoded_hume, decoded_wind = struct.unpack('>HBB', encoded_data)
        decoded_temp = (decoded_temp / 100) - 50
        decoded_hume = int(decoded_hume)
        wind_directions = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
        decoded_wind = wind_directions[decoded_wind]

        decoded_data = {'temperatura': decoded_temp, 'humedad': decoded_hume, 'direccion_viento': decoded_wind}
        return decoded_data

    def start_kafka_consumer(self):
        try:
            while True:
                message = self.consumer.poll(1.0)

                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        print("No más mensajes en la partición.")
                    else:
                        print(f"Error en el mensaje: {message.error()}")
                else:
                    decoded_payload = self.decode_data(message.value())
                    self.all_temp.append(decoded_payload['temperatura'])
                    self.all_hume.append(decoded_payload['humedad'])
                    self.all_wind.append(decoded_payload['direccion_viento'])

                    print(f"Datos recibidos: {decoded_payload}")

                    self.plot_all_data()
        except KeyboardInterrupt:
            print("Interrupción del usuario. Cerrando el consumidor Kafka.")
        finally:
            self.consumer.close()

    def plot_all_data(self):
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        ax.plot(self.all_temp, label='Temperatura', color='purple')
        ax.plot(self.all_hume, label='Humedad', color='pink')
        ax.legend()
        ax.set_xlabel('Muestras')
        ax.set_ylabel('Valores')
        ax.set_title('Telemetría en Vivo')
        self.canvas.draw()

# Configura el servidor y el topic de Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '201055'

# Crear una ventana de Tkinter
root = tk.Tk()
root.title('Laboratorio9-Redes')

# Crear y iniciar el consumidor de Kafka en un hilo
kafka_consumer = KafkaDataConsumer(bootstrap_servers, topic)
kafka_thread = threading.Thread(target=kafka_consumer.start_kafka_consumer)
kafka_thread.daemon = True
kafka_thread.start()

# Iniciar la aplicación de Tkinter
root.mainloop()
