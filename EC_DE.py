import socket
import threading
import time
from kafka import KafkaProducer, KafkaConsumer

#CONSTANTES

#CENTRAL_IP = '127.0.0.1'
#CENTRAL_PORT = 8000
TAXI_ID = 1
SENSORS_PORT = 12346

taxi_pos = [1, 1]
taxi_status = 'OK'

# Kafka config
BROKER = 'localhost:9092'
TOPIC_TAXI_STATUS = f'taxi_status_{TAXI_ID}'  # Topic para enviar status del taxi
TOPIC_CENTRAL_COMMANDS = f'central_commands_{TAXI_ID}'  # Topic para recibir comandos de la central

producer = KafkaProducer(bootstrap_servers=BROKER)
consumer = KafkaConsumer(TOPIC_CENTRAL_COMMANDS, bootstrap_servers=BROKER, group_id=f'taxi_{TAXI_ID}')


'''
def conectar_central():
    global taxi_status
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CENTRAL_IP, CENTRAL_PORT))
            s.sendall(f'AUTH {TAXI_ID}'.encode())
            respuesta = s.recv(1024).decode()
            if respuesta == 'OK':
                print(f'Taxi {TAXI_ID} autenticado con la central.')
                while True:
                    # Esperar comandos desde Kafka (central)
                    for message in consumer:
                        data = message.value.decode()
                        print(f'Central Kafka: {data}')
                        if data == 'MOVE':
                            mover_taxi()
                        elif data == 'STOP':
                            taxi_status = 'KO'
                        elif data == 'RESUME':
                            taxi_status = 'OK'
                    time.sleep(1)
            else:
                print('Autenticación fallida.')
    except Exception as e:
        print(f'Error de conexión con la central: {e}')
'''
def mover_taxi():
    global taxi_pos, taxi_status
    while taxi_status == 'OK':
        taxi_pos[0] += 1
        print(f'Taxi {TAXI_ID} se mueve a posición {taxi_pos}')
        # Publicar el estado actual en Kafka
        #producer.send(TOPIC_TAXI_STATUS, f'MOVE {taxi_pos}'.encode())
        time.sleep(2)

def escuchar_sensores():
    global taxi_status
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sensor_socket:
        sensor_socket.bind(('0.0.0.0', SENSORS_PORT))
        sensor_socket.listen(1)
        print(f'Esperando conexión de sensores en el puerto {SENSORS_PORT}...')
        conn, addr = sensor_socket.accept()
        with conn:
            print(f'Conexión de sensores recibida desde {addr}')
            while True:
                sensor_data = conn.recv(1024).decode()
                if sensor_data:
                    print(f'Sensor: {sensor_data}')
                    if sensor_data == 'KO':
                        taxi_status = 'KO'
                        print(f'Taxi {TAXI_ID} detenido por el sensor.')
                        #producer.send(TOPIC_TAXI_STATUS, f'KO'.encode())
                    elif sensor_data == 'OK':
                        taxi_status = 'OK'
                        print(f'Taxi {TAXI_ID} reanudado por el sensor.')
                        #producer.send(TOPIC_TAXI_STATUS, f'OK'.encode())
                time.sleep(1)

# Iniciar los hilos
#threading.Thread(target=conectar_central).start()
threading.Thread(target=escuchar_sensores).start()
