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
TAXI_ID = config["taxi"]["taxi_id"]
SENSORS_PORT = config["taxi"]["sensors_port"]
BROKER = config["taxi"]["broker"]
TOPIC_TAXI_STATUS = f'taxi_status_{TAXI_ID}'  # Tópico dinámico según el ID del taxi
TOPIC_CENTRAL_COMMANDS = f'central_commands_{TAXI_ID}'  # Tópico dinámico según el ID del taxi

# Parámetros de la central
CENTRAL_IP = config["central"]["ip"]
CENTRAL_PORT = config["central"]["port"]

# Kafka config
producer = KafkaProducer(bootstrap_servers=BROKER)
consumer = KafkaConsumer(TOPIC_CENTRAL_COMMANDS, bootstrap_servers=BROKER, group_id=f'taxi_{TAXI_ID}')

taxi_pos = [1, 1]  # Posición inicial
taxi_status = 'OK'  # Estado inicial del taxi

def autenticar_con_central():
    """Autentica el taxi con la central mediante sockets."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CENTRAL_IP, CENTRAL_PORT))
            # Enviar el ID del taxi para autenticación
            s.sendall(f"{TAXI_ID}".encode())
            
            # Esperar la respuesta de la central
            respuesta = s.recv(1024).decode()
            if respuesta == "Autenticación exitosa":
                print(f"Taxi {TAXI_ID} autenticado con éxito.")
                return True
            else:
                print(f"Taxi {TAXI_ID} autenticación fallida: {respuesta}")
                return False
    except Exception as e:
        print(f"Error de conexión con la central: {e}")
        return False

def mover_taxi_hacia(destino_x, destino_y):
    """Función para mover el taxi hacia la posición de destino."""
    global taxi_pos, taxi_status

    while True:
        if taxi_pos[0] == destino_x and taxi_pos[1] == destino_y:
            print(f'Taxi {TAXI_ID} ha llegado al destino {taxi_pos}')
            break

        if taxi_status == 'KO':  # Si el estado es KO, detenerse
            print(f'Taxi {TAXI_ID} detenido, esperando cambio de estado a OK...')
            time.sleep(2)
            continue

        # Calcular la diferencia en los ejes X e Y
        delta_x = destino_x - taxi_pos[0]
        if abs(delta_x) > 10:  # Ajuste por movimiento circular en X
            delta_x = delta_x - 20 if delta_x > 0 else delta_x + 20

        delta_y = destino_y - taxi_pos[1]
        if abs(delta_y) > 10:  # Ajuste por movimiento circular en Y
            delta_y = delta_y - 20 if delta_y > 0 else delta_y + 20

        # Mover en X
        if delta_x != 0:
            taxi_pos[0] += 1 if delta_x > 0 else -1

        # Mover en Y
        if delta_y != 0:
            taxi_pos[1] += 1 if delta_y > 0 else -1

        # Envolver el mapa cuando se sale de los límites
        taxi_pos[0] = (taxi_pos[0] - 1) % 20 + 1
        taxi_pos[1] = (taxi_pos[1] - 1) % 20 + 1

        print(f'Taxi {TAXI_ID} se mueve a posición {taxi_pos}')
        # Enviar el estado del movimiento a Kafka
        producer.send(TOPIC_TAXI_STATUS, f'MOVE {taxi_pos}'.encode())

        time.sleep(2)

def escuchar_sensores():
    """Función para escuchar las señales de los sensores conectados al taxi."""
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

# Iniciar los hilos si la autenticación con la central es exitosa
if autenticar_con_central():
    threading.Thread(target=escuchar_sensores).start()
    threading.Thread(target=mover_taxi_hacia, args=(10, 10)).start()  # Prueba de mover a una posición destino
else:
    print("No se pudo autenticar con la central, terminando proceso.")
