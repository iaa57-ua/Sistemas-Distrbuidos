import socket
import json
import threading
from kafka import KafkaProducer, KafkaConsumer
import sys
import colorama
from colorama import Fore, Style

colorama.init(autoreset=True)

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

# Definir el tópico de estado de taxis (por ejemplo, usando un patrón para múltiples taxis)
TOPIC_TAXI_STATUS_PATTERN = 'taxiStatus'  # Regex para escuchar todos los tópicos de estado de taxis

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

# Tamaño del mapa y su inicialización
TAMANO_MAPA = 20
mapa = [[["."] for _ in range(20)] for _ in range(20)]
LINE = f"{'-' * 100}\n"

# Cargar configuraciones de ubicaciones y clientes del archivo
locations = {loc["Id"]: list(map(int, loc["POS"].split(','))) for loc in config["locations"]}


def actualizar_mapa(tipo, id_, posicion, estado=None):
    x, y = posicion
    if tipo == "taxi":
        # Representa el taxi con el ID en el color correspondiente
        color = Fore.GREEN if estado == "verde" else Fore.RED
        mapa[x - 1][y - 1] = f"{color}{id_}{Style.RESET_ALL}"  # Actualiza la celda en el mapa
    elif tipo == "cliente":
        color = Fore.YELLOW
        mapa[x - 1][y - 1] = f"{color}{id_}{Style.RESET_ALL}"
    elif tipo == "destino":
        mapa[x - 1][y - 1] = "D"
    
    pintar_mapa()

def leer_base_datos(file_path='bdd.txt'):
    """Lee el archivo de la base de datos y devuelve un diccionario con la información de cada taxi."""
    taxis = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                taxi_id, libre, estado, coord_x_destino, coord_y_destino = line.strip().split(',')
                origen_taxi = config["taxi"]["posicion_inicial"]
                taxi_id = int(taxi_id)
                libre = libre.strip().lower() == 'si'
                taxis[taxi_id] = {
                    "libre": libre,
                    "estado": estado.strip(),
                    "origen": origen_taxi,
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
        if taxi_id in taxis_activos:
            # Cambiar el estado a disponible y color rojo al autenticarse
            taxis_activos[taxi_id]["libre"] = True
            taxis_activos[taxi_id]["estado"] = "rojo"
            print(f"Taxi {taxi_id} autenticado con éxito y disponible en estado 'rojo'.")
            connection.send("Autenticación exitosa".encode())
            
            # Obtener posición inicial desde la base de datos o usar una por defecto
            posicion_inicial = taxis_activos[taxi_id].get("origen")  # Posición por defecto en (1,1)
            actualizar_mapa("taxi", taxi_id, posicion_inicial, estado="rojo")  # Actualiza el mapa con el taxi en rojo
            actualizar_base_datos(taxis_activos)  # Actualizar base de datos
        else:
            print(f"Taxi {taxi_id} rechazado. No está activo o no registrado.")
            connection.send("Autenticación fallida".encode())
    except ValueError:
        print(f"Error: Se esperaba un número para el ID del taxi, pero se recibió: {data}")
    finally:
        connection.close()

# Seguir aqui
def escuchar_actualizaciones_taxi(taxis_activos):
    """Escucha actualizaciones de posición de los taxis en Kafka y actualiza el mapa en tiempo real."""
    print("Central escuchando actualizaciones de posición de los taxis en Kafka...")
    
    # Consumidor de Kafka con patrón de tópico
    consumer_status = KafkaConsumer(TOPIC_TAXI_STATUS_PATTERN)

    for mensaje in consumer_status:
        data = mensaje.value
        taxi_id = data.get("taxi_id")
        nueva_pos = data.get("pos")
        print("izan va a perder todas y merecido")
        if taxi_id and nueva_pos:
            estado_taxi = taxis_activos[taxi_id]["estado"]
            actualizar_mapa("taxi", taxi_id, nueva_pos, estado=estado_taxi)  # Actualiza el mapa
            print(f"Central actualizó posición del Taxi {taxi_id} a {nueva_pos}")



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
            taxis_activos[taxi_id]["estado"] = "verde"
            taxis_activos[taxi_id]["destino"] = solicitud["destino"]
            
            #Confirmar asignación al cliente
            print(f"Asignando taxi {taxi_id} al cliente {solicitud['client_id']} con estado 'verde'.")
            producer.send(TOPIC_CONFIRMATION, {"client_id": solicitud["client_id"], "mensaje": f"Taxi {taxi_id} asignado"})
            producer.flush()

            # Enviar ubicación de recogida y destino al taxi
            enviar_destino_a_taxi(taxi_id, solicitud["ubicacion_actual"], solicitud["destino"])

            # Actualizar base de datos después de la asignación
            actualizar_base_datos(taxis_activos)
            
            #actualizar_mapa("taxi", taxi_id, , estado=estado_taxi)
            return
    
    print("No hay taxis libres disponibles")

def liberar_taxi(taxi_id, taxis_activos):
    """Libera un taxi cuando ha terminado su recorrido."""
    taxis_activos[taxi_id]["libre"] = True
    taxis_activos[taxi_id]["destino"] = (None, None)
    print(f"Taxi {taxi_id} liberado y listo para nuevas asignaciones.")

    # Actualizar base de datos después de liberar el taxi
    actualizar_base_datos(taxis_activos)

    
# Función para enviar el destino al taxi a través de Kafka
def enviar_destino_a_taxi(taxi_id, ubicacion_cliente, destino):
    topic_taxi_commands = f'central_commands_{taxi_id}'
    mensaje = {
        "ubicacion_cliente": ubicacion_cliente,  # Ubicación de recogida como lista de enteros
        "destino_final": destino,  # Destino final también en lista de enteros
        "asignado" : True
    }
    producer.send(topic_taxi_commands, value=mensaje)
    producer.flush()
    print(f"Ubicación del cliente {ubicacion_cliente} y destino {destino} enviados al taxi {taxi_id} en el tópico {topic_taxi_commands}")


def escuchar_peticiones_cliente(taxis_activos):
    """Escucha continuamente las peticiones de los clientes en Kafka y procesa cada solicitud."""
    print("Central escuchando peticiones de clientes en Kafka...")
    for mensaje in consumer:
        solicitud = mensaje.value
        print(f"Solicitud de cliente recibida: {solicitud}")
        actualizar_mapa("cliente", config["cliente"]["client_id"], config["cliente"]["ubicacion_inicial"], None)
        asignar_taxi(solicitud, taxis_activos)

def manejar_llegada_destino(mensaje):
    """Procesa la llegada de un taxi al destino del cliente."""
    taxi_id = mensaje["taxi_id"]
    destino = mensaje["destino"]

    # Notificar al cliente que el taxi ha llegado
    mensaje_cliente = {
        "client_id": taxi_id,  # Este puede ser el ID del taxi para simplificación
        "mensaje": f"Taxi {taxi_id} ha llegado a su destino {destino}"
    }
    producer.send(TOPIC_CONFIRMATION, value=mensaje_cliente)
    producer.flush()

    # Actualizar estado y disponibilidad del taxi en la base de datos
    taxis_activos[taxi_id]["libre"] = True
    taxis_activos[taxi_id]["estado"] = "rojo"
    taxis_activos[taxi_id]["destino"] = (None, None)
    actualizar_base_datos(taxis_activos)


#Usar en casos que cambiemos el estado de un taxi
def actualizar_base_datos(taxis_activos, file_path='bdd.txt'):
    with open(file_path, 'w') as f:
        for taxi_id, datos in taxis_activos.items():
            estado = "si" if datos["libre"] else "no"
            f.write(f"{taxi_id}, {estado}, {datos['estado']}, {datos['destino'][0] or '-'}, {datos['destino'][1] or '-'}\n")
    print("Base de datos actualizada.")

EMPTY = "."

def mostrar_mapa():
    """Imprime el mapa actual, incluyendo taxis, clientes y destinos predefinidos."""
    print("Mapa de la ciudad:")
    for fila in mapa:
        print(" ".join(fila))
    print("\n")

def pintar_mapa_inicial():
    """Dibuja el mapa inicial y coloca las ubicaciones predefinidas desde `locations`."""
    print("\n" * 5)
    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<20} *** EASY CAB Release 1 ***\n")
    sys.stdout.write(LINE)

    # Encabezado de taxis y clientes
    sys.stdout.write(f"{' ':<20} {'Taxis':<29} {'|':<20} {'Clientes'}\n")
    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<8} {'Id.':<10} {'Destino':<15} {'Estado':<14} {'|':<7} {'Id.':<10} {'Destino':<15} {'Estado':<15}\n")
    sys.stdout.write(LINE)

    sys.stdout.write(LINE + "\n")
    sys.stdout.write("   " + " ".join([f"{i:2}" for i in range(1, 21)]) + "\n")  # Ajuste de ancho
    sys.stdout.write(LINE + "\n")

    # Limpiar el mapa y poner los puntos de interés
    for x in range(20):
        for y in range(20):
            mapa[x][y] = EMPTY  # Cada celda como un punto

    # Colocar ubicaciones desde `locations`
    for loc_id, (x, y) in locations.items():
        mapa[x - 1][y - 1] = Fore.BLUE + loc_id + Style.RESET_ALL # Identificación de ubicación por ID

    # Imprimir filas del mapa con ubicaciones
    for row in range(20):
        sys.stdout.write(f"{row + 1:<2} ")
        for col in range(20):
            sys.stdout.write(f"{mapa[row][col]:<2} ")
        sys.stdout.write("\n")  # Nueva línea después de cada fila

    sys.stdout.write(LINE + "\n")
    sys.stdout.flush()


def pintar_mapa():
    """Dibuja el mapa inicial y coloca las ubicaciones predefinidas desde `locations`."""
    print("\n" * 5)
    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<20} *** EASY CAB Release 1 ***\n")
    sys.stdout.write(LINE)

    # Encabezado de taxis y clientes
    sys.stdout.write(f"{' ':<20} {'Taxis':<29} {'|':<20} {'Clientes'}\n")
    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<8} {'Id.':<10} {'Destino':<15} {'Estado':<14} {'|':<7} {'Id.':<10} {'Destino':<15} {'Estado':<15}\n")
    sys.stdout.write(LINE)

    sys.stdout.write(LINE + "\n")
    sys.stdout.write("   " + " ".join([f"{i:2}" for i in range(1, 21)]) + "\n")  # Ajuste de ancho
    sys.stdout.write(LINE + "\n")

    # Imprimir filas del mapa con ubicaciones
    for row in range(20):
        sys.stdout.write(f"{row + 1:<2} ")
        for col in range(20):
            sys.stdout.write(f"{mapa[row][col]:<2} ")
        sys.stdout.write("\n")  # Nueva línea después de cada fila

    sys.stdout.write(LINE + "\n")
    sys.stdout.flush()

# Modificar la función main para iniciar el escucha de posición en paralelo
def main():
    taxis_activos = leer_base_datos()
    pintar_mapa_inicial()  # Dibuja el mapa al inicio mostrando ubicaciones

    # Hilos para manejar conexiones de taxis y escuchar posiciones
    threading.Thread(target=iniciar_socket_taxi, args=(taxis_activos,)).start()
    threading.Thread(target=escuchar_peticiones_cliente, args=(taxis_activos,)).start()
    threading.Thread(target=escuchar_actualizaciones_taxi, args=(taxis_activos,)).start()

if __name__ == '__main__':
    main()