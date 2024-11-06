import socket
import threading
import time
import json
from kafka import KafkaProducer, KafkaConsumer

# Cargar la configuración desde un archivo JSON
def cargar_configuracion(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

config = cargar_configuracion('config.json')

# Parámetros de taxi y central
TAXI_ID = config["taxi"]["taxi_id"]
SENSORS_PORT = config["taxi"]["sensors_port"]
CENTRAL_IP = config["central"]["ip"]
CENTRAL_PORT = config["central"]["port"]
BROKER = config["taxi"]["broker"]
TOPIC_TAXI_ESTADO = 'taxiEstado'
TOPIC_TAXI_STATUS = 'taxiStatus'
TOPIC_TAXI_COMMANDS = f'central_commands_{TAXI_ID}'  # Tópico donde recibe el destino desde la central

producer = KafkaProducer(bootstrap_servers=BROKER)
consumer = KafkaConsumer(
    TOPIC_TAXI_COMMANDS,
    bootstrap_servers=BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'taxi_{TAXI_ID}',
    auto_offset_reset='latest'  # Escuchar solo mensajes nuevos
)

taxi_pos = config["taxi"]["posicion_inicial"]  # Posición inicial
taxi_status = 'OK'  # Estado inicial del taxi

def conectar_con_sensor():
    """Espera y se conecta al sensor en el puerto configurado."""
    try:
        sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sensor_socket.bind(('0.0.0.0', SENSORS_PORT))
        sensor_socket.listen(1)
        print(f"Esperando la conexión del sensor en el puerto {SENSORS_PORT}...")
        conn, addr = sensor_socket.accept()
        ready_message = conn.recv(1024).decode()
        if ready_message == str(TAXI_ID):
            print(f"Sensor del taxi {TAXI_ID} conectado.")
            return conn  # Devuelve la conexión establecida
        else:
            print(f"Error: Sensor del taxi {TAXI_ID} no se pudo conectar.")
            conn.close()
            return None
    except Exception as e:
        print(f"Error al conectar con el sensor: {e}")
        return None

def autenticar_con_central():
    """Autentica el taxi con la central después de conectar con el sensor."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((CENTRAL_IP, CENTRAL_PORT))
            s.sendall(f"{TAXI_ID}".encode())
            respuesta = s.recv(1024).decode()
            if respuesta == "Autenticación exitosa":
                print(f"Taxi {TAXI_ID} autenticado con éxito en la central.")
                return True
            else:
                print(f"Taxi {TAXI_ID} autenticación fallida: {respuesta}")
                return False
    except socket.timeout:
        print("Error de conexión: El tiempo de espera de conexión ha expirado.")
    except Exception as e:
        print(f"Error de conexión con la central: {e}")
    return False

# Modificar la función mover_taxi_hacia para enviar posición del taxi
def mover_taxi_hacia(destino_x, destino_y):
    """Función para mover el taxi hacia la posición de destino en un mapa circular de 20x20."""
    global taxi_pos, taxi_status

    print(f"Iniciando movimiento hacia el destino: [{destino_x}, {destino_y}]")
    print(f"Taxi inicia en posición: {taxi_pos}")

    # Cambiar estado a verde al iniciar el movimiento
#     actualizar_estado_en_central("verde")
    
    while True:
        actualizar_estado_en_central("verde")
        # Si el taxi está en KO, cambia el estado a "rojo" en la central y espera
        if taxi_status == 'KO':
            actualizar_estado_en_central("rojo")  # Reflejar el estado en la central
            print(f'Taxi {TAXI_ID} en estado KO, esperando cambio a OK...')
            producer.send(TOPIC_TAXI_STATUS, f"TAXI_{TAXI_ID}_KO".encode())  # Notificar KO a Kafka
            producer.flush()
            time.sleep(2)
            continue
        
        # Verificar si ha alcanzado el destino
        if taxi_pos == [destino_x, destino_y]:
            print(f'Taxi {TAXI_ID} ha llegado al destino {taxi_pos}')
            producer.send(TOPIC_TAXI_STATUS, f"Taxi {TAXI_ID} ha llegado al destino {taxi_pos}".encode())
            producer.flush()
            actualizar_estado_en_central("rojo")  # Cambiar a "rojo" al llegar al destino
            break
        
        # Calcular delta con movimiento circular en X
        delta_x = (destino_x - taxi_pos[0]) % 20
        if delta_x > 10:
            delta_x -= 20

        # Calcular delta con movimiento circular en Y
        delta_y = (destino_y - taxi_pos[1]) % 20
        if delta_y > 10:
            delta_y -= 20

        # Mover en X
        if delta_x != 0:
            taxi_pos[0] = (taxi_pos[0] + (1 if delta_x > 0 else -1)) % 20 or 20

        # Mover en Y
        if delta_y != 0:
            taxi_pos[1] = (taxi_pos[1] + (1 if delta_y > 0 else -1)) % 20 or 20

        print(f'Taxi {TAXI_ID} se mueve a posición {taxi_pos}')
        
        # Enviar actualización de posición a la central
        mensaje_posicion = {
            "taxi_id": TAXI_ID,
            "posicion": taxi_pos
        }
        producer.send(TOPIC_TAXI_STATUS, json.dumps(mensaje_posicion).encode())  # Notificar posición a Kafka
        producer.flush()
        
        time.sleep(2)



def realizar_recorrido(ubicacion_cliente, destino_final):
    """Gestiona el proceso de recoger al cliente y luego llevarlo a su destino final."""
    # Asegurarse de que destino_final sea una lista de enteros
    if isinstance(destino_final, str):
        destino_final = list(map(int, destino_final.split(',')))

    print(f"Taxi {TAXI_ID} dirigiéndose a la ubicación del cliente en {ubicacion_cliente} para recogerlo.")
    
    # Ir a la ubicación del cliente
    mover_taxi_hacia(*ubicacion_cliente)
    print(f"Taxi {TAXI_ID} ha llegado a la ubicación del cliente en {ubicacion_cliente}")
    producer.send(TOPIC_TAXI_STATUS, f"Taxi {TAXI_ID} ha llegado a la ubicación del cliente en {ubicacion_cliente}".encode())
    producer.flush()

    # Indicar que el cliente ha subido al taxi
    print(f"Cliente ha subido al taxi {TAXI_ID}")
    producer.send(TOPIC_TAXI_STATUS, f"Cliente ha subido al taxi {TAXI_ID}".encode())
    producer.flush()

    # Llevar al cliente al destino final
    print(f"Taxi {TAXI_ID} llevando al cliente al destino {destino_final}")
    mover_taxi_hacia(*destino_final)
    
    # Notificar a la central que ha llegado al destino final
    print(f"Taxi {TAXI_ID} ha llegado al destino final {destino_final}")
    mensaje_llegada = {
        "taxi_id": TAXI_ID,
        "destino": destino_final,
        "llegada": True
    }
    producer.send(TOPIC_TAXI_STATUS, json.dumps(mensaje_llegada).encode())  # Enviar llegada a la central
    producer.flush()
    
    # Poner el taxi en estado libre y rojo
    actualizar_estado_en_central("rojo")
    taxi_pos = config["taxi"]["posicion_inicial"]  # Reiniciar posición al punto inicial



def escuchar_sensores(conn):
    global taxi_status
    while True:
        try:
            sensor_data = conn.recv(1024).decode()
            if sensor_data:
                print(f'Sensor envió: {sensor_data}')
                taxi_status = sensor_data
                if taxi_status == 'KO':
                    actualizar_estado_en_central("rojo")
                elif taxi_status == 'OK':  # Si vuelve a OK y está en movimiento
                    actualizar_estado_en_central("verde")
        except (ConnectionResetError, ConnectionAbortedError):
            print("Conexión con el sensor perdida. Intentando reconectar...")
            conn.close()
            time.sleep(1)
            conn = conectar_con_sensor()
            if not conn:
                print("No se pudo reconectar al sensor. Terminando escucha.")
                break
        except Exception as e:
            print(f"Error inesperado en la comunicación con el sensor: {e}")
            break

def escuchar_destino():
    """Escucha el destino desde Kafka y mueve el taxi hacia allí solo cuando recibe una solicitud de destino válida."""
    print(f"Esperando destino en el tópico {TOPIC_TAXI_COMMANDS}...")

    # Asegurarse de que el consumidor esté suscrito y tenga particiones asignadas
    consumer.subscribe([TOPIC_TAXI_COMMANDS])
    consumer.poll(0)  # Forzar la suscripción inmediata

    # Espera hasta que se asignen particiones
    while not consumer.assignment():
        print("Esperando asignación de particiones...")
        time.sleep(0.5)

    # Limpia mensajes residuales para evitar movimientos no deseados
    consumer.seek_to_end()  # Coloca el consumidor al final del tópico

    # Procesa solo mensajes nuevos y válidos
    for message in consumer:
        comando = message.value
        print(f"Mensaje recibido de la central: {comando}")

        # Verifica que el mensaje tenga la estructura esperada y la solicitud sea confirmada
        if (isinstance(comando, dict) and 
            "ubicacion_cliente" in comando and 
            "destino_final" in comando and 
            comando.get("asignado", False) == True):  # Verifica si está asignado

            ubicacion_cliente = comando["ubicacion_cliente"]
            destino_final = comando["destino_final"]

            # Asegúrate de que destino_final esté en el formato correcto
            if isinstance(destino_final, str):
                destino_final = list(map(int, destino_final.split(',')))

            # Mueve el taxi a la ubicación del cliente primero
            print(f"Destino de recogida recibido: {ubicacion_cliente}")
            mover_taxi_hacia(ubicacion_cliente[0], ubicacion_cliente[1])

            # Luego, al destino final
            print(f"Destino final recibido: {destino_final}")
            mover_taxi_hacia(destino_final[0], destino_final[1])

def actualizar_estado_en_central(color):
    """Envía el color actual del taxi (rojo o verde) a la central para actualización en la base de datos."""
    mensaje = {
        "taxi_id": TAXI_ID,
        "estado": color,
        "pos": taxi_pos
    }
    producer.send(TOPIC_TAXI_ESTADO, json.dumps(mensaje).encode())
    producer.flush()


# Proceso principal
sensor_conn = conectar_con_sensor()
if sensor_conn:
    if autenticar_con_central():
        threading.Thread(target=escuchar_sensores, args=(sensor_conn,)).start()
        threading.Thread(escuchar_destino()).start()
        #escuchar_destino()  # Escuchar el destino desde Kafka
else:
    print(f"Taxi {TAXI_ID}: No se pudo conectar al sensor, terminando proceso.")