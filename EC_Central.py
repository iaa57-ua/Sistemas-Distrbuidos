import socket
import json
import threading
from kafka import KafkaProducer, KafkaConsumer

# Cargar configuración desde el archivo JSON
def cargar_configuracion(file_path='config.json'):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

# Cargar configuraciones desde config.json
config = cargar_configuracion()

# Parámetros de conexión de la central y Kafka
CENTRAL_IP = config["central"]["ip"]
CENTRAL_PORT = config["central"]["port"]
BROKER = config["taxi"]["broker"]
TOPIC_REQUEST_TAXI = config["cliente"]["topic_request_taxi"]
TOPIC_CONFIRMATION = config["cliente"]["topic_confirmation"]

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

# Diccionario para almacenar el estado y las configuraciones de los taxis
taxis = {}

def iniciar_socket_sensor():
    """Inicia el socket para recibir conexiones de sensores y autenticarlos."""
    sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sensor_socket.bind((CENTRAL_IP, CENTRAL_PORT))
    sensor_socket.listen(5)
    print(f"Central esperando sensores en {CENTRAL_IP}:{CENTRAL_PORT}...")

    while True:
        connection, address = sensor_socket.accept()
        print(f"Conexión entrante del sensor desde {address}")
        # Iniciar un hilo para manejar la conexión del sensor
        threading.Thread(target=manejar_conexion_sensor, args=(connection,)).start()

def manejar_conexion_sensor(connection):
    """Maneja la interacción con el sensor tras la conexión inicial."""
    try:
        mensaje = connection.recv(1024).decode()
        if "READY" in mensaje:
            taxi_id = int(mensaje.split()[1])  # Obtener el ID del taxi desde el mensaje
            print(f"Taxi {taxi_id} conector.")
            taxis[taxi_id] = {"status": "ready"}
        else:
            print(f"Sensor no está listo: {mensaje}")
    except Exception as e:
        print(f"Error en la comunicación con el sensor: {e}")

def procesar_solicitudes_clientes():
    """Escucha y procesa las solicitudes de los clientes."""
    print(f"Esperando solicitudes de clientes en el tópico {TOPIC_REQUEST_TAXI}...")
    for mensaje in consumer:
        solicitud = mensaje.value
        print(f"Solicitud recibida: {solicitud}")
        manejar_solicitud_cliente(solicitud)

def manejar_solicitud_cliente(solicitud):
    """Maneja la solicitud de un cliente para asignar un taxi."""
    ubicacion_actual = solicitud['ubicacion_actual']
    destino = solicitud['destino']
    
    taxi_asignado = None
    for taxi_id, info in taxis.items():
        if info['status'] == 'ready':
            taxi_asignado = taxi_id
            break
    
    if taxi_asignado:
        print(f"Asignando taxi {taxi_asignado} a la solicitud del cliente {solicitud['client_id']}")
        taxis[taxi_asignado]['status'] = 'occupied'
        comando = {"action": "move", "from": ubicacion_actual, "to": destino}
        producer.send(config["taxi"]["topic_central_commands"], comando)
        confirmacion = {"client_id": solicitud['client_id'], "mensaje": f"Taxi {taxi_asignado} asignado."}
        producer.send(TOPIC_CONFIRMATION, confirmacion)
    else:
        print("No hay taxis disponibles.")

def main():
    taxis_activos = cargar_configuracion()
    threading.Thread(target=iniciar_socket_sensor).start()
    threading.Thread(target=procesar_solicitudes_clientes).start()

if __name__ == '__main__':
    main()
