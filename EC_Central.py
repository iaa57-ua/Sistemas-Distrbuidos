import socket
import json
import threading
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

# Parámetros de configuración de Kafka
TOPIC_REQUEST_TAXI = config["cliente"]["topic_request_taxi"]
TOPIC_CONFIRMATION = config["cliente"]["topic_confirmation"]
BROKER = config["taxi"]["broker"]

# Puerto y dirección del socket para autenticación
CENTRAL_SOCKET_IP = config["central"]["ip"]
CENTRAL_SOCKET_PORT = config["central"]["port"]

# Crear el productor de Kafka para enviar confirmaciones a clientes
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

def leer_base_datos(file_path='bdd.txt'):
    """Lee el archivo de la base de datos y devuelve un diccionario con la información de cada taxi."""
    taxis = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                taxi_id, libre, estado, coord_x_origen, coord_y_origen, coord_x_destino, coord_y_destino = line.strip().split(',')
                taxi_id = int(taxi_id)
                libre = libre.strip().lower() == 'si'
                taxis[taxi_id] = {
                    "libre": libre,
                    "estado": estado.strip(),
                    "posicion_actual": (int(coord_x_origen), int(coord_y_origen)),
                    "destino": (None if coord_x_destino.strip() == '-' else int(coord_x_destino),
                                None if coord_y_destino.strip() == '-' else int(coord_y_destino))
                }
    except FileNotFoundError:
        print("El archivo de base de datos no existe.")
    except Exception as e:
        print(f"Error al leer la base de datos: {e}")
    return taxis

def autenticar_taxi(taxi_id, taxis_activos):
    """Autentica un taxi comparando su ID contra los datos de la base de datos."""
    if taxi_id in taxis_activos and taxis_activos[taxi_id]["libre"]:
        print(f"Taxi {taxi_id} autenticado con éxito.")
        return True
    else:
        print(f"Taxi {taxi_id} rechazado. No está activo o no registrado.")
        return False

def manejar_conexion_taxi(connection, taxis_activos):
    """Maneja la conexión con un taxi a través de sockets."""
    try:
        data = connection.recv(1024).decode().strip()
        taxi_id = int(data)
        if autenticar_taxi(taxi_id, taxis_activos):
            connection.send("Autenticación exitosa".encode())
        else:
            connection.send("Autenticación fallida".encode())
    except ValueError:
        print(f"Error: Se esperaba un número para el ID del taxi, pero se recibió: {data}")
    finally:
        connection.close()

def iniciar_socket_taxi(taxis_activos):
    """Inicia el socket para recibir conexiones de taxis y autenticarlos."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((CENTRAL_SOCKET_IP, CENTRAL_SOCKET_PORT))
    server_socket.listen(5)
    print(f"Central esperando taxis en {CENTRAL_SOCKET_IP}:{CENTRAL_SOCKET_PORT}...")
    
    while True:
        connection, address = server_socket.accept()
        print(f"Conexión entrante de {address}")
        threading.Thread(target=manejar_conexion_taxi, args=(connection, taxis_activos)).start()

def asignar_taxi(solicitud, taxis_activos):
    """Busca un taxi libre y lo asigna a la solicitud del cliente."""
    for taxi_id, datos in taxis_activos.items():
        if datos["libre"]:
            taxis_activos[taxi_id]["libre"] = False
            taxis_activos[taxi_id]["destino"] = solicitud["destino"]
            print(f"Asignando taxi {taxi_id} al cliente {solicitud['client_id']}")

            # Enviar confirmación al cliente
            producer.send(TOPIC_CONFIRMATION, {"client_id": solicitud["client_id"], "mensaje": f"Taxi {taxi_id} asignado"})
            producer.flush()
            
            # Enviar el destino al taxi a través de Kafka
            enviar_destino_a_taxi(taxi_id, solicitud["destino"])
            return
    
    print("No hay taxis libres disponibles")

    
# Función para enviar el destino al taxi a través de Kafka
def enviar_destino_a_taxi(taxi_id, destino):
    topic_taxi_commands = f'central_commands_{taxi_id}'
    mensaje = {"destino": destino}
    producer.send(topic_taxi_commands, value=mensaje)
    producer.flush()
    print(f"Destino {destino} enviado al taxi {taxi_id} en el tópico {topic_taxi_commands}")


def escuchar_peticiones_cliente(taxis_activos):
    """Escucha continuamente las peticiones de los clientes en Kafka y procesa cada solicitud."""
    print("Central escuchando peticiones de clientes en Kafka...")
    for mensaje in consumer:
        solicitud = mensaje.value
        print(f"Solicitud de cliente recibida: {solicitud}")
        asignar_taxi(solicitud, taxis_activos)

def main():
    taxis_activos = leer_base_datos()
    threading.Thread(target=iniciar_socket_taxi, args=(taxis_activos,)).start()
    escuchar_peticiones_cliente(taxis_activos)

if __name__ == '__main__':
    main()
