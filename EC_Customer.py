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

# Parámetros de configuración del cliente
BROKER = config["taxi"]["broker"]
TOPIC_REQUEST_TAXI = config["cliente"]["topic_request_taxi"]
TOPIC_CONFIRMATION = config["cliente"]["topic_confirmation"]
CLIENT_ID = config["cliente"]["client_id"]

# Configurar el Kafka Producer para enviar mensajes
producer = KafkaProducer(bootstrap_servers=BROKER)

# Configurar el Kafka Consumer para recibir mensajes
consumer = KafkaConsumer(
    TOPIC_CONFIRMATION,
    bootstrap_servers=BROKER,
    group_id=f'client_{CLIENT_ID}',
    auto_offset_reset='earliest'
)

def solicitar_taxi(ubicacion_actual, destino):
    """Envía una solicitud de taxi a la central."""
    solicitud = {
        "client_id": CLIENT_ID,
        "ubicacion_actual": ubicacion_actual,
        "destino": destino
    }
    print(f"Cliente {CLIENT_ID} solicitando taxi desde {ubicacion_actual} hacia {destino}")
    producer.send(TOPIC_REQUEST_TAXI, json.dumps(solicitud).encode('utf-8'))
    producer.flush()

def esperar_confirmacion():
    # Esperar confirmación del taxi
    print(f"Cliente {CLIENT_ID} esperando confirmación...")
    while True:
        try:
            for message in consumer:
                confirmacion = json.loads(message.value.decode())
                if confirmacion["client_id"] == CLIENT_ID:
                    print(f"Cliente {CLIENT_ID} ha recibido confirmación: {confirmacion['mensaje']}")
                    return
        except Exception as e:
            print(f"Error al procesar confirmación: {e}")
            break

if __name__ == "__main__":
    # Solicitar taxi desde la ubicación (10, 5) hacia la ubicación (18, 12)
    solicitar_taxi([10, 5], [18, 12])
    
    # Esperar la confirmación de que un taxi ha sido asignado
    esperar_confirmacion()
