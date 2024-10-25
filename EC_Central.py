import socket
import json
import threading
import time
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

# Diccionario de taxis
taxis = {}

def leer_base_datos(file_path='bdd.txt'):
    """Lee el archivo de la base de datos y devuelve un diccionario con la información de cada taxi."""
    taxis = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                # Dividir la línea en los valores esperados
                taxi_id, libre, estado, coord_x_origen, coord_y_origen, coord_x_destino, coord_y_destino = line.strip().split(',')

                # Convertir valores a los tipos necesarios
                taxi_id = int(taxi_id)
                libre = libre.strip().lower() == 'si'
                coord_x_origen = int(coord_x_origen)
                coord_y_origen = int(coord_y_origen)
                
                # Si no tiene destino, asignar `None`
                coord_x_destino = None if coord_x_destino.strip() == '-' else int(coord_x_destino)
                coord_y_destino = None if coord_y_destino.strip() == '-' else int(coord_y_destino)
                
                # Guardar la información del taxi en el diccionario
                taxis[taxi_id] = {
                    "libre": libre,
                    "estado": estado.strip(),
                    "posicion_actual": (coord_x_origen, coord_y_origen),
                    "destino": (coord_x_destino, coord_y_destino)
                }
    except FileNotFoundError:
        print("El archivo de base de datos no existe.")
    except Exception as e:
        print(f"Error al leer la base de datos: {e}")
    return taxis

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
        escuchar_peticiones_cliente() # Con esto escuha las peticiones que le envia el cliente


def escuchar_peticiones_cliente():
    """Escucha continuamente las peticiones de los clientes en Kafka y procesa cada solicitud."""
    print("Central escuchando peticiones de clientes en Kafka...")
    
    # Consumir mensajes de Kafka en el tópico de solicitud de taxis
    for mensaje in consumer:
        try:
            solicitud = mensaje.value  # Obtener el valor del mensaje
            print(f"Central recibió solicitud de cliente: {solicitud}")

            # Llamar a la función para asignar un taxi en base a la solicitud recibida
            
        
        except Exception as e:
            print(f"Error al procesar la solicitud de cliente: {e}")


def main():
    """Función principal que inicia el sistema de la central."""
    taxis_activos = leer_base_datos()  # Leer la base de datos para obtener los taxis activos
    iniciar_socket_taxi(taxis_activos)

if __name__ == '__main__':
    main()
