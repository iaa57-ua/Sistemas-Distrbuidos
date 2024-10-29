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

# Cargar configuración
config = cargar_configuracion('config.json')

# Parámetros de Kafka y cliente
BROKER = config["taxi"]["broker"]
TOPIC_REQUEST_TAXI = config["cliente"]["topic_request_taxi"]
TOPIC_CONFIRMATION = config["cliente"]["topic_confirmation"]
CLIENT_ID = config["cliente"]["client_id"]
UBICACION = config["cliente"]["ubicacion_inicial"]

producer = KafkaProducer(bootstrap_servers=BROKER)
consumer = KafkaConsumer(
    TOPIC_CONFIRMATION,
    bootstrap_servers=BROKER,
    group_id=f'client_{CLIENT_ID}',
    auto_offset_reset='earliest'
)

# Diccionario de ubicaciones (destinos) desde la configuración
locations = {loc["Id"]: list(map(int, loc["POS"].split(','))) for loc in config["locations"]}

def solicitar_taxi(destino):
    """Envía una solicitud de taxi con la ubicación actual y el destino especificado."""
    solicitud = {
        "client_id": CLIENT_ID,
        "ubicacion_actual": UBICACION,
        "destino": destino
    }
    print(f"Cliente {CLIENT_ID} solicitando taxi desde {UBICACION} hacia {destino}")
    producer.send(TOPIC_REQUEST_TAXI, json.dumps(solicitud).encode('utf-8'))
    producer.flush()

def esperar_confirmacion_llegada():
    """Espera la confirmación de que el taxi ha llegado al destino final."""
    print("Esperando confirmación de llegada al destino...")
    for message in consumer:
        confirmacion = json.loads(message.value.decode())
        
        if confirmacion.get("client_id") == CLIENT_ID:
            mensaje = confirmacion.get("mensaje", "")
            print(f"Cliente '{CLIENT_ID}' recibió mensaje: {mensaje}")
            
            if "ha llegado a su destino" in mensaje:
                print("Cliente ha llegado a su destino. Solicitará el siguiente en 10 segundos.")
                break  # Sale del bucle cuando recibe la confirmación de llegada

def solicitar_destinos():
    """Solicita taxis secuencialmente para los destinos en la lista 'Requests'."""
    requests = config["cliente"]["Requests"]
    
    for request in requests:
        destino_id = request["Id"]
        
        # Verificar si el destino existe en las ubicaciones definidas
        if destino_id in locations:
            destino = locations[destino_id]
            solicitar_taxi(destino)  # Enviar solicitud al destino actual
            esperar_confirmacion_llegada()  # Espera a que el taxi confirme la llegada
            time.sleep(10)  # Espera 10 segundos antes de solicitar el siguiente destino
        else:
            print(f"Destino '{destino_id}' no encontrado en las ubicaciones.")

if __name__ == "__main__":
    solicitar_destinos()
