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
def mover_taxi_hacia(destino_x, destino_y):
    global taxi_pos, taxi_status

    while True:  # Bucle para mover el taxi mientras tenga destino
        # Verifica si el taxi ha llegado al destino
        if taxi_pos[0] == destino_x and taxi_pos[1] == destino_y:
            print(f'Taxi {TAXI_ID} ha llegado al destino {taxi_pos}')
            break  # El taxi llega al destino, termina el bucle

        # Espera mientras el estado sea "KO"
        if taxi_status == 'KO':
            print(f'Taxi {TAXI_ID} detenido, esperando cambio de estado a OK...')
            time.sleep(2)  # Esperar antes de verificar el estado nuevamente
            continue  # Volver al inicio del bucle para verificar el estado

        # Si el estado es "OK", el taxi puede continuar moviéndose
        # Calcular la distancia en X
        delta_x = destino_x - taxi_pos[0]
        if abs(delta_x) > 10:  # Distancia circular en el eje X
            delta_x = delta_x - 20 if delta_x > 0 else delta_x + 20

        # Calcular la distancia en Y
        delta_y = destino_y - taxi_pos[1]
        if abs(delta_y) > 10:  # Distancia circular en el eje Y
            delta_y = delta_y - 20 if delta_y > 0 else delta_y + 20

        # Mover en el eje X si es necesario
        if delta_x != 0:
            taxi_pos[0] += 1 if delta_x > 0 else -1

        # Mover en el eje Y si es necesario
        if delta_y != 0:
            taxi_pos[1] += 1 if delta_y > 0 else -1

        # Envolver el taxi cuando pasa los límites del mapa
        taxi_pos[0] = (taxi_pos[0] - 1) % 20 + 1  # Mantener el taxi en [1, 20] en X
        taxi_pos[1] = (taxi_pos[1] - 1) % 20 + 1  # Mantener el taxi en [1, 20] en Y

        print(f'Taxi {TAXI_ID} se mueve a posición {taxi_pos}')
        # Aquí enviarías la posición actual del taxi a Kafka
        # producer.send(TOPIC_TAXI_STATUS, f'MOVE {taxi_pos}'.encode())

        time.sleep(2)  # Esperar antes de la siguiente iteración para verificar el estado



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
                    elif sensor_data == 'OK':
                        taxi_status = 'OK'
                        print(f'Taxi {TAXI_ID} reanudado por el sensor.')
                time.sleep(1)



# Iniciar los hilos
#threading.Thread(target=conectar_central).start()
threading.Thread(target=escuchar_sensores).start()

# Pruebas de movimientos circulares
destinos = [(16, 5), (9, 9), (11, 11), (17, 16)]

for destino in destinos:
    print(f"Probando movimiento hacia {destino}")
    mover_taxi_hacia(destino[0], destino[1])
