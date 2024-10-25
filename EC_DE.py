import socket
import threading
import time
import json
from kafka import KafkaProducer, KafkaConsumer

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

# Parámetros de taxi y central
TAXI_ID = config["taxi"]["taxi_id"]
SENSORS_PORT = config["taxi"]["sensors_port"]  
CENTRAL_IP = config["central"]["ip"]
CENTRAL_PORT = config["central"]["port"]

# Kafka config
BROKER = config["taxi"]["broker"]
TOPIC_TAXI_STATUS = f'taxi_status_{TAXI_ID}'  # Tópico dinámico según el ID del taxi
TOPIC_CENTRAL_COMMANDS = f'central_commands_{TAXI_ID}'  # Tópico dinámico según el ID del taxi

producer = KafkaProducer(bootstrap_servers=BROKER)
consumer = KafkaConsumer(TOPIC_CENTRAL_COMMANDS, bootstrap_servers=BROKER, group_id=f'taxi_{TAXI_ID}')

taxi_pos = [1, 1]  # Posición inicial
taxi_status = 'OK'  # Estado inicial del taxi

def conectar_con_sensor():
    """Espera y se conecta al sensor en el puerto configurado."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sensor_socket:
        sensor_socket.bind(('0.0.0.0', SENSORS_PORT))  # Puerto del sensor
        sensor_socket.listen(1)
        print(f"Esperando la conexión del sensor en el puerto {SENSORS_PORT}...")
        conn, addr = sensor_socket.accept()
        with conn:
            ready_message = conn.recv(1024).decode()
            if "READY" in ready_message:
                print(f"Sensor del taxi {TAXI_ID} conectado.")
                return True
            else:
                print(f"Error: Sensor del taxi {TAXI_ID} no se pudo conectar.")
                return False

def autenticar_con_central():
    """Autentica el taxi con la central después de conectar con el sensor."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CENTRAL_IP, CENTRAL_PORT))
            s.sendall(f"{TAXI_ID}".encode())
            respuesta = s.recv(1024).decode()
            if respuesta == "Autenticación exitosa":
                print(f"Taxi {TAXI_ID} autenticado con éxito en la central.")
                # Enviar mensaje a Kafka
                producer.send(TOPIC_TAXI_STATUS, f"Taxi {TAXI_ID} conectado exitosamente.".encode())
                producer.flush()
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

# Iniciar el proceso principal de conexión y autenticación
if conectar_con_sensor():  # Intentar conectar al sensor primero
    if autenticar_con_central():  # Si el sensor está conectado, autenticar con la central
        threading.Thread(target=escuchar_sensores).start()
else:
    print(f"Taxi {TAXI_ID}: No se pudo conectar al sensor, terminando proceso.")
