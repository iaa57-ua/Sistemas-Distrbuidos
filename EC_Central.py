import socket
import json
import threading
import time
from kafka import KafkaProducer, KafkaConsumer

# Variable global para gestionar el ID de los taxis
taxi_id_counter = 0

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

# Parámetros de configuración de Kafka
TOPIC_REQUEST_TAXI = config["cliente"]["topic_request_taxi"]
TOPIC_CONFIRMATION = config["cliente"]["topic_confirmation"]
BROKER = config["taxi"]["broker"]

# Puerto y dirección del socket para autenticación
CENTRAL_SOCKET_IP = config["central"]["ip"]
CENTRAL_SOCKET_PORT = config["central"]["port"]

# Crear el productor de Kafka para enviar comandos a los taxis y confirmaciones a los clientes
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Crear el consumidor de Kafka para recibir solicitudes de los clientes
consumer = KafkaConsumer(
    TOPIC_REQUEST_TAXI,
    bootstrap_servers=BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Diccionario de taxis y solicitudes pendientes
taxis = {}  # Diccionario para almacenar el estado y ubicación de los taxis


def leer_base_datos(file_path='bdd.txt'):
    """Lee el archivo de la base de datos y devuelve un diccionario con los taxis activos."""
    taxis_activos = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                taxi_id, activo = line.strip().split(',')
                taxis_activos[int(taxi_id)] = (activo == 'True')
    except FileNotFoundError:
        print("El archivo de base de datos no existe.")
    return taxis_activos


def autenticar_taxi(taxi_id, taxis_activos):
    """Autentica un taxi comparando su ID contra los datos de la base de datos."""
    if taxi_id in taxis_activos and taxis_activos[taxi_id]:
        print(f"Taxi {taxi_id} autenticado con éxito.")
        return True
    else:
        print(f"Taxi {taxi_id} rechazado. No está activo o no registrado.")
        return False

def manejar_conexion_taxi(connection, taxis_activos):
    """Maneja la conexión con un taxi a través de sockets."""
    try:
        # Leer el ID del taxi desde la conexión
        taxi_id = int(connection.recv(1024).decode())

        if autenticar_taxi(taxi_id, taxis_activos):
            connection.send("Autenticación exitosa".encode())

            # Crear tópicos únicos para cada taxi usando el taxi_id recibido
            topic_sensor = f"taxi_sensor_{taxi_id}"
            topic_commands = f"taxi_command_{taxi_id}"

            taxis[taxi_id] = {
                "status": "free",
                "position": [0, 0],
                "sensor_topic": topic_sensor,
                "command_topic": topic_commands
            }

            # Iniciar el hilo para manejar los sensores del taxi
            threading.Thread(target=simular_sensor, args=(taxi_id, topic_sensor)).start()
        else:
            connection.send("Autenticación fallida".encode())
    except Exception as e:
        print(f"Error al manejar la conexión del taxi: {e}")
    finally:
        connection.close()


def simular_sensor(taxi_id, topic_sensor):
    """Simula un sensor que envía la posición del taxi periódicamente."""
    while taxis[taxi_id]["status"] != "stopped":
        posicion = taxis[taxi_id]["position"]
        producer.send(topic_sensor, {"taxi_id": taxi_id, "position": posicion})
        producer.flush()
        # Simular sensor enviando cada 5 segundos
        time.sleep(5)


def iniciar_socket(taxis_activos):
    """Inicia el socket para recibir conexiones de taxis y autenticarlos."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((CENTRAL_SOCKET_IP, CENTRAL_SOCKET_PORT))
    server_socket.listen(5)
    print(f"Central esperando taxis en {CENTRAL_SOCKET_IP}:{CENTRAL_SOCKET_PORT}...")
    
    while True:
        connection, address = server_socket.accept()
        print(f"Conexión entrante de {address}")
        threading.Thread(target=manejar_conexion_taxi, args=(connection, taxis_activos)).start()


def manejar_solicitud_cliente(solicitud):
    """Maneja la solicitud de un cliente para asignar un taxi."""
    ubicacion_actual = solicitud['ubicacion_actual']
    destino = solicitud['destino']
    
    # Buscar un taxi libre
    taxi_asignado = None
    for taxi_id, info in taxis.items():
        if info['status'] == 'free':
            taxi_asignado = taxi_id
            break
    
    if taxi_asignado is not None:
        print(f"Asignando taxi {taxi_asignado} a la solicitud del cliente {solicitud['client_id']}")
        taxis[taxi_asignado]['status'] = 'ocupado'

        # Enviar comando al taxi para que se mueva
        comando = {
            "action": "move",
            "from": ubicacion_actual,
            "to": destino
        }
        producer.send(taxis[taxi_asignado]['command_topic'], comando)

        # Notificar al cliente que un taxi ha sido asignado
        confirmacion = {
            "client_id": solicitud['client_id'],
            "mensaje": f"Taxi {taxi_asignado} asignado."
        }
        producer.send(TOPIC_CONFIRMATION, confirmacion)
    else:
        print("No hay taxis libres disponibles.")
        # También puedes enviar una notificación al cliente si no hay taxis disponibles.


def procesar_solicitudes_clientes():
    """Función que escucha y procesa las solicitudes de los clientes."""
    print(f"Esperando solicitudes de clientes en el tópico {TOPIC_REQUEST_TAXI}...")
    for mensaje in consumer:
        solicitud = mensaje.value
        print(f"Solicitud recibida: {solicitud}")
        manejar_solicitud_cliente(solicitud)


def main():
    """Función principal que inicia el sistema de la central."""
    taxis_activos = leer_base_datos()  # Leer la base de datos para obtener los taxis activos
    
    # Iniciar autenticación de taxis y procesamiento de solicitudes de clientes en paralelo
    threading.Thread(target=iniciar_socket, args=(taxis_activos,)).start()
    threading.Thread(target=procesar_solicitudes_clientes).start()


# Ejecutar la central
if __name__ == '__main__':
    main()
