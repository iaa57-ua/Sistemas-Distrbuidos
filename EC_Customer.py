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

config = cargar_configuracion('config.json')

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

def solicitar_taxi(destino):
    solicitud = {
        "client_id": CLIENT_ID,
        "ubicacion_actual": UBICACION,
        "destino": destino
    }
    print(f"Cliente {CLIENT_ID} solicitando taxi desde {UBICACION} hacia {destino}")
    producer.send(TOPIC_REQUEST_TAXI, json.dumps(solicitud).encode('utf-8'))
    producer.flush()

def esperar_confirmacion():
    """Espera confirmaciones del taxi asignado a medida que se actualizan las etapas del recorrido."""
    print(f"Cliente {CLIENT_ID} esperando confirmación...")
    recogido = False  # Estado de si el cliente ya fue recogido

    for message in consumer:
        confirmacion = json.loads(message.value.decode())
        
        if confirmacion["client_id"] == CLIENT_ID:
            mensaje = confirmacion["mensaje"]
            print(f"Cliente {CLIENT_ID} recibió mensaje: {mensaje}")
            
            # Mensaje cuando el taxi llega a recoger al cliente
            if "ha llegado a la ubicación del cliente" in mensaje and not recogido:
                print("Cliente subiendo al taxi.")
                recogido = True
            
            # Mensaje cuando el taxi ha llegado al destino final
            elif "ha llegado al destino final" in mensaje and recogido:
                print("Cliente ha llegado a su destino final. Fin del recorrido.")
                break  # Finaliza el bucle cuando el cliente ha llegado



if __name__ == "__main__":
    #Pedimos coordenas al usuario
    entrada = input("Indique el destino x,y: ")
    x, y = map(int, entrada.split(','))  # Convertir a enteros
    solicitar_taxi([x, y])  # Pasar el destino como lista de enteros

    # Esperar la confirmación de que un taxi ha sido asignado y ha llegado
    esperar_confirmacion()
