import socket
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import json

# Función para cargar parámetros
def cargar_configuracion(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

# Cargar la configuración desde un archivo JSON
config = cargar_configuracion('config.json')

# Usar parámetros de la configuración
TAXI_ID = config["taxi_id"]
SENSORS_PORT = config["sensors_port"]
BROKER = config["broker"]
TOPIC_TAXI_STATUS = config["topic_taxi_status"]
TOPIC_CENTRAL_COMMANDS = config["topic_central_commands"]

# Kafka config
producer = KafkaProducer(bootstrap_servers=BROKER)
consumer = KafkaConsumer(TOPIC_CENTRAL_COMMANDS, bootstrap_servers=BROKER, group_id=f'taxi_{TAXI_ID}')

def mover_taxi_hacia(destino_x, destino_y):
    global taxi_pos, taxi_status

    while True:
        if taxi_pos[0] == destino_x and taxi_pos[1] == destino_y:
            print(f'Taxi {TAXI_ID} ha llegado al destino {taxi_pos}')
            break

        if taxi_status == 'KO':
            print(f'Taxi {TAXI_ID} detenido, esperando cambio de estado a OK...')
            time.sleep(2)
            continue

        delta_x = destino_x - taxi_pos[0]
        if abs(delta_x) > 10:
            delta_x = delta_x - 20 if delta_x > 0 else delta_x + 20

        delta_y = destino_y - taxi_pos[1]
        if abs(delta_y) > 10:
            delta_y = delta_y - 20 if delta_y > 0 else delta_y + 20

        if delta_x != 0:
            taxi_pos[0] += 1 if delta_x > 0 else -1

        if delta_y != 0:
            taxi_pos[1] += 1 if delta_y > 0 else -1

        taxi_pos[0] = (taxi_pos[0] - 1) % 20 + 1
        taxi_pos[1] = (taxi_pos[1] - 1) % 20 + 1

        print(f'Taxi {TAXI_ID} se mueve a posición {taxi_pos}')
        producer.send(TOPIC_TAXI_STATUS, f'MOVE {taxi_pos}'.encode())

        time.sleep(2)

def escuchar_sensores():
    global taxi_status
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sensor_socket:
        sensor_socket.bind(('0.0.0.0', SENSORS_PORT))
        sensor_socket.listen(1)
        print(f'Esperando conexión de sensores en el puerto {SENSORS_PORT}...')
        conn, addr = sensor_socket.accept()
        with conn:
            print(f'Conexión de sensores recibida desde {addr}')
            while True:
                sensor_data = conn.recv(1024).decode()
                if sensor_data:
                    print(f'Sensor: {sensor_data}')
                    if sensor_data == 'KO':
                        taxi_status = 'KO'
                        print(f'Taxi {TAXI_ID} detenido por el sensor.')
                    elif sensor_data == 'OK':
                        taxi_status = 'OK'
                        print(f'Taxi {TAXI_ID} reanudado por el sensor.')
                time.sleep(1)

# Iniciar los hilos
threading.Thread(target=escuchar_sensores).start()
