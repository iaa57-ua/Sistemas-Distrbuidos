from kafka import KafkaProducer, KafkaConsumer
import json
import time

# Definir los temas de Kafka
TOPIC_TAXI_STATUS = 'taxi_status'
TOPIC_CENTRAL_COMMANDS = 'central_commands'
TOPIC_REQUEST_TAXI = 'taxi_requests'
TOPIC_CONFIRMATION = 'taxi_confirmation'

# Crear el productor de Kafka para enviar comandos a los taxis y confirmaciones a los clientes
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Crear consumidores de Kafka para escuchar los estados de los taxis y las peticiones de clientes
consumer_taxi = KafkaConsumer(
    TOPIC_TAXI_STATUS,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='central',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

consumer_customer = KafkaConsumer(
    TOPIC_REQUEST_TAXI,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='central',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Estado del sistema
taxis = {}  # Diccionario para almacenar el estado y ubicación de los taxis
pending_requests = []  # Lista para almacenar las solicitudes de clientes pendientes

def escuchar_actualizaciones_taxis():
    """Escucha los estados de los taxis y actualiza su posición."""
    print("Esperando actualizaciones de taxis...")
    for message in consumer_taxi:
        taxi_data = message.value
        taxi_id = taxi_data['taxi_id']
        taxis[taxi_id] = taxi_data
        print(f"Taxi {taxi_id} - Nueva posición: {taxi_data['position']} - Estado: {taxi_data['status']}")
        # Aquí puedes manejar más lógica sobre los taxis (paradas, reanudaciones, etc.)

def escuchar_solicitudes_clientes():
    """Escucha las solicitudes de clientes y asigna taxis libres."""
    print("Esperando solicitudes de clientes...")
    for message in consumer_customer:
        request_data = message.value
        print(f"Nueva solicitud del cliente {request_data['client_id']} para destino: {request_data['destino']}")
        pending_requests.append(request_data)
        asignar_taxi(request_data)

def asignar_taxi(request_data):
    """Asignar un taxi libre a la solicitud del cliente."""
    for taxi_id, taxi_data in taxis.items():
        if taxi_data['status'] == 'free':  # Se asume que el taxi está libre si su estado es 'free'
            print(f"Asignando Taxi {taxi_id} a la solicitud del cliente {request_data['client_id']}.")
            enviar_comando_taxi(taxi_id, request_data['destino'])
            # Envía confirmación al cliente
            enviar_confirmacion_cliente(request_data['client_id'], f"Taxi {taxi_id} asignado.")
            return
    print("No hay taxis disponibles en este momento.")
    # Si no hay taxis disponibles, enviar un mensaje de no disponibilidad
    enviar_confirmacion_cliente(request_data['client_id'], "No hay taxis disponibles en este momento.")

def enviar_comando_taxi(taxi_id, destino):
    """Enviar un comando al taxi para que recoja al cliente y lo lleve a su destino."""
    command = {
        'taxi_id': taxi_id,
        'command': 'go_to',
        'destination': destino
    }
    producer.send(TOPIC_CENTRAL_COMMANDS, command)
    producer.flush()
    print(f"Enviando taxi {taxi_id} al destino {destino}")

def enviar_confirmacion_cliente(client_id, mensaje):
    """Enviar confirmación de taxi al cliente."""
    confirmacion = {
        'client_id': client_id,
        'mensaje': mensaje
    }
    producer.send(TOPIC_CONFIRMATION, json.dumps(confirmacion).encode())
    producer.flush()
    print(f"Enviada confirmación a cliente {client_id}: {mensaje}")

def main():
    """Función principal que inicia el proceso de escucha para taxis y clientes."""
    while True:
        escuchar_actualizaciones_taxis()  # Escucha las actualizaciones de los taxis
        escuchar_solicitudes_clientes()  # Escucha las solicitudes de los clientes
        time.sleep(1)

# Ejecutar el sistema central
if __name__ == '__main__':
    main()
