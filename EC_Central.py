import socket
import json
from kafka import KafkaProducer, KafkaConsumer

# Configuraciones de Kafka
TOPIC_TAXI_STATUS = 'taxi_status'
TOPIC_CENTRAL_COMMANDS = 'central_commands'
TOPIC_REQUEST_TAXI = 'taxi_requests'
TOPIC_CONFIRMATION = 'taxi_confirmation'

# Puerto y dirección del socket para autenticación
CENTRAL_SOCKET_IP = 'localhost'
CENTRAL_SOCKET_PORT = 9999

# Crear el productor de Kafka para enviar comandos a los taxis y confirmaciones a los clientes
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
        data = connection.recv(1024).decode()
        taxi_id = int(data)
        if autenticar_taxi(taxi_id, taxis_activos):
            connection.send("Autenticación exitosa".encode())
            taxis[taxi_id] = {"status": "free", "position": [0, 0]}  # Taxi se agrega al sistema como libre
        else:
            connection.send("Autenticación fallida".encode())
    except Exception as e:
        print(f"Error al manejar la conexión del taxi: {e}")
    finally:
        connection.close()

def iniciar_socket(taxis_activos):
    """Inicia el socket para recibir conexiones de taxis y autenticarlos."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((CENTRAL_SOCKET_IP, CENTRAL_SOCKET_PORT))
    server_socket.listen(5)  # Escuchar hasta 5 conexiones simultáneas
    print(f"Central esperando taxis en {CENTRAL_SOCKET_IP}:{CENTRAL_SOCKET_PORT}...")
    
    while True:
        connection, address = server_socket.accept()
        print(f"Conexión entrante de {address}")
        manejar_conexion_taxi(connection, taxis_activos)

def main():
    """Función principal que inicia el sistema de la central."""
    taxis_activos = leer_base_datos()  # Leer la base de datos para obtener los taxis activos
    if iniciar_socket(taxis_activos): # Iniciar el socket de autenticación
        print("Autentificación verificada")
    else:
        print("Autentificación fallida")     

# Ejecutar la central
if __name__ == '__main__':
    main()
