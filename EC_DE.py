import socket
import threading
import time
import json
from kafka import KafkaProducer

# Cargar la configuración desde un archivo JSON
def cargar_configuracion(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

config = cargar_configuracion('config.json')

# Parámetros de taxi y central
TAXI_ID = config["taxi"]["taxi_id"]
SENSORS_PORT = config["taxi"]["sensors_port"]
CENTRAL_IP = config["central"]["ip"]
CENTRAL_PORT = config["central"]["port"]
BROKER = config["taxi"]["broker"]
TOPIC_TAXI_STATUS = f'taxi_status_{TAXI_ID}'

producer = KafkaProducer(bootstrap_servers=BROKER)
taxi_pos = [1, 1]  # Posición inicial
taxi_status = 'OK'  # Estado inicial del taxi

def conectar_con_sensor():
    """Espera y se conecta al sensor en el puerto configurado."""
    try:
        sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sensor_socket.bind(('0.0.0.0', SENSORS_PORT))
        sensor_socket.listen(1)
        print(f"Esperando la conexión del sensor en el puerto {SENSORS_PORT}...")
        conn, addr = sensor_socket.accept()
        ready_message = conn.recv(1024).decode()
        if ready_message == str(TAXI_ID):
            print(f"Sensor del taxi {TAXI_ID} conectado.")
            return conn  # Devuelve la conexión establecida
        else:
            print(f"Error: Sensor del taxi {TAXI_ID} no se pudo conectar.")
            conn.close()
            return None
    except Exception as e:
        print(f"Error al conectar con el sensor: {e}")
        return None

def autenticar_con_central():
    """Autentica el taxi con la central después de conectar con el sensor."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CENTRAL_IP, CENTRAL_PORT))
            s.sendall(f"{TAXI_ID}".encode())
            respuesta = s.recv(1024).decode()
            if respuesta == "Autenticación exitosa":
                print(f"Taxi {TAXI_ID} autenticado con éxito en la central.")
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
        if taxi_pos == [destino_x, destino_y]:
            print(f'Taxi {TAXI_ID} ha llegado al destino {taxi_pos}')
            break

        if taxi_status == 'KO':
            print(f'Taxi {TAXI_ID} detenido, esperando cambio de estado a OK...')
            time.sleep(2)
            continue

        # Movimiento en X e Y, ajustado para mapa circular
        delta_x = (destino_x - taxi_pos[0]) % 20
        if delta_x > 10:
            delta_x -= 20
        delta_y = (destino_y - taxi_pos[1]) % 20
        if delta_y > 10:
            delta_y -= 20

        taxi_pos[0] = (taxi_pos[0] + (1 if delta_x > 0 else -1)) % 20 or 20
        taxi_pos[1] = (taxi_pos[1] + (1 if delta_y > 0 else -1)) % 20 or 20

        print(f'Taxi {TAXI_ID} se mueve a posición {taxi_pos}')
        producer.send(TOPIC_TAXI_STATUS, f'MOVE {taxi_pos}'.encode())
        time.sleep(2)

def escuchar_sensores(conn):
    """Escucha las señales de los sensores y ajusta el estado del taxi (OK/KO)."""
    global taxi_status
    with conn:
        print(f'Conexión establecida con el sensor en el puerto {SENSORS_PORT}')
        while True:
            try:
                sensor_data = conn.recv(1024).decode()
                if sensor_data:
                    print(f'Sensor envió: {sensor_data}')
                    taxi_status = sensor_data
            except (ConnectionResetError, ConnectionAbortedError):
                print("Conexión con el sensor perdida.")
                conn.close()
                break
            except Exception as e:
                print(f"Error inesperado en la comunicación con el sensor: {e}")
                break

# Proceso principal
sensor_conn = conectar_con_sensor()
if sensor_conn:
    if autenticar_con_central():
        threading.Thread(target=escuchar_sensores, args=(sensor_conn,)).start()
        #mover_taxi_hacia(14, 14)
        #time.sleep(5)
        #mover_taxi_hacia(8, 8)
else:
    print(f"Taxi {TAXI_ID}: No se pudo conectar al sensor, terminando proceso.")
