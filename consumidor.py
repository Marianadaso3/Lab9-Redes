import threading
from confluent_kafka import Consumer, KafkaError
import tkinter as tk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json
import struct

# Configura el servidor y el topic de Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '201055'

# Configura el consumidor Kafka
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'  # Lee desde el inicio del topic
}

consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Listas para almacenar los datos
all_temp = []
all_hume = []
all_wind = []

# Función para procesar y graficar datos

def plot_all_data():
    figure.clear()
    ax = figure.add_subplot(111)
    ax.plot(all_temp, label='Temperatura')
    ax.plot(all_hume, label='Humedad')
    ax.legend()
    ax.set_xlabel('Muestras')
    ax.set_ylabel('Valores')
    ax.set_title('Telemetría en Vivo')
    canvas.draw()


# Crear una ventana de Tkinter
root = tk.Tk()
root.title('Telemetría en Vivo')

# Crear una figura de Matplotlib
figure = Figure(figsize=(5, 4), dpi=100)

# Crear un lienzo para la figura
canvas = FigureCanvasTkAgg(figure, master=root)
canvas.get_tk_widget().pack()

# Función para iniciar el consumo de Kafka en un hilo

# aqui haremos los cambios para el proceso de decode y encode para el ejercicio 3.4
def encode_data(data):
    encoded_temp = int((data['temperatura'] + 50) * 100)  # Rango de temperatura: -50 a 50
    encoded_hume = int(data['humedad'])
    # Codificar la dirección del viento
    wind_directions = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
    encoded_wind = wind_directions.index(data['direccion_viento'])

    # Empaquetar los datos en 3 bytes (24 bits)
    encoded_data = struct.pack('>HBB', encoded_temp, encoded_hume, encoded_wind)

    return encoded_data


def decode_data(encoded_data):
    # Desempaquetar los datos
    decoded_temp, decoded_hume, decoded_wind = struct.unpack('>HBB', encoded_data)

    # Decodificar los datos
    decoded_temp = (decoded_temp / 100) - 50  # Deshacer la codificación de temperatura
    decoded_hume = int(decoded_hume)
    # Decodificar la dirección del viento
    wind_directions = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
    decoded_wind = wind_directions[decoded_wind]

    decoded_data = {'temperatura': decoded_temp, 'humedad': decoded_hume, 'direccion_viento': decoded_wind}
    return decoded_data


def start_kafka_consumer():
    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    print("No más mensajes en la partición.")
                else:
                    print(f"Error en el mensaje: {message.error()}")
            else:
                # Decodificar el mensaje recibido
                decoded_payload = decode_data(message.value())
                all_temp.append(decoded_payload['temperatura'])
                all_hume.append(decoded_payload['humedad'])
                all_wind.append(decoded_payload['direccion_viento'])

                print(f"Datos recibidos: {decoded_payload}")

                # Grafica los datos en tiempo real
                plot_all_data()
    except KeyboardInterrupt:
        print("Interrupción del usuario. Cerrando el consumidor Kafka.")
    finally:
        consumer.close()


# Iniciar el consumidor de Kafka en un hilo
kafka_thread = threading.Thread(target=start_kafka_consumer)
kafka_thread.daemon = True
kafka_thread.start()

# Iniciar la aplicación de Tkinter
root.mainloop()
