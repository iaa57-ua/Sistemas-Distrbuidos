import socket
import json
import threading
from kafka import KafkaProducer, KafkaConsumer
import sys
import colorama
from colorama import Fore, Style
import time


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
BROKER = config["taxi"]["broker"] #ip y puerto kafka
PUERTO_ESCUCHA = config["central"]["port"]

# Definir el tópico de estado de taxis (por ejemplo, usando un patrón para múltiples taxis)
TOPIC_TAXI_STATUS_PATTERN = 'taxiEstado'  # Regex para escuchar todos los tópicos de estado de taxis

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

#Diccionario que nos ayudará a guardar el id del cliente con su ubicación actual
solicitudes_pendientes = {}
pos_final = ("-","-")

def actualizar_mapa(tipo, id_, posicion, estado=None):
    x, y = posicion
    if tipo == "taxi":
        # Determina el color en función del estado
        color = Fore.GREEN if estado == "verde" else Fore.RED

        # Limpia la posición anterior del taxi en el mapa
        for fila in range(20):
            for col in range(20):
                if mapa[fila][col] == f"{Fore.GREEN}{id_}{Style.RESET_ALL}" or mapa[fila][col] == f"{Fore.RED}{id_}{Style.RESET_ALL}":
                    mapa[fila][col] = EMPTY

        # Actualiza las ubicaciones en el mapa
        for loc_id, (a, b) in locations.items():
            mapa[a - 1][b - 1] = Fore.BLUE + loc_id + Style.RESET_ALL
        
        # Representa el taxi con el cliente si hay uno en la base de datos
        cliente = taxis_activos[id_].get("cliente")
        if cliente:
            
            for fila in range(20):
                for col in range(20):
                    if mapa[fila][col] == f"{color}{id_}{cliente}{Style.RESET_ALL}":
                        mapa[fila][col] = EMPTY
                        
            mapa[x - 1][y - 1] = f"{color}{id_}{cliente}{Style.RESET_ALL}"
        else:
            mapa[x - 1][y - 1] = f"{color}{id_}{Style.RESET_ALL}"
    
    elif tipo == "cliente":
        color = Fore.YELLOW
        mapa[x - 1][y - 1] = f"{color}{id_}{Style.RESET_ALL}"

    # Llama a la función para mostrar el mapa actualizado
    pintar_mapa()
    


def leer_base_datos(file_path='bdd.txt'):
    """Lee el archivo de la base de datos y devuelve un diccionario con la información de cada taxi."""
    taxis = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                taxi_id, libre, estado, coord_x_destino, coord_y_destino, cliente = line.strip().split(',')
                origen_taxi = config["taxi"]["posicion_inicial"]
                taxi_id = int(taxi_id)
                libre = libre.strip().lower() == 'si'
                taxis[taxi_id] = {
                    "libre": libre,
                    "estado": estado.strip(),
                    "origen": origen_taxi,
                    "destino": (None if coord_x_destino.strip() == '-' else int(coord_x_destino),
                                None if coord_y_destino.strip() == '-' else int(coord_y_destino)),
                    "cliente": None if cliente == '-' else cliente
                }
    except FileNotFoundError:
        print("El archivo de base de datos no existe.")
    except Exception as e:
        print(f"Error al leer la base de datos: {e}")
    return taxis

def autenticar_taxi(taxi_id, taxis_activos):
    """Autentica un taxi comparando su ID contra los datos de la base de datos."""
    if taxi_id in taxis_activos:
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
            # Cambiar el estado a disponible y color rojo al autenticarse
            taxis_activos[taxi_id]["libre"] = True
            taxis_activos[taxi_id]["estado"] = "rojo"
            taxis_activos[taxi_id]["destino"] = (None,None)
            taxis_activos[taxi_id]["cliente"] = None
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

def escuchar_actualizaciones_taxi(taxis_activos):
    global pos_final
    """Escucha actualizaciones de posición de los taxis en Kafka y actualiza el mapa en tiempo real."""
    print("Central escuchando actualizaciones de posición de los taxis en Kafka...")
    
    # Consumidor de Kafka con patrón de tópico
    consumer_status = KafkaConsumer(TOPIC_TAXI_STATUS_PATTERN)

    for mensaje in consumer_status:
        data = json.loads(mensaje.value.decode('utf-8'))
        print(f"Datos recibidos: {data}")
        taxi_id = data["taxi_id"]
        nueva_pos = data["pos"]
        
        if taxi_id and nueva_pos:
            estado_taxi = taxis_activos[taxi_id]["estado"]
            
            for client_id, ubicacion_cliente in solicitudes_pendientes.items():
                if nueva_pos == ubicacion_cliente:
                    #cliente se monta
                    taxis_activos[taxi_id]["cliente"] = client_id
                    actualizar_mapa("taxi", taxi_id, nueva_pos, estado=estado_taxi)  # Actualiza el mapa
                    
                    #del solicitudes_pendientes[client_id]
                    print(f"Cliente '{client_id} subió al taxi {taxi_id}")
                    actualizar_base_datos(taxis_activos)
            
            actualizar_mapa("taxi", taxi_id, nueva_pos, estado=estado_taxi)
            print(f"Central actualizó posición del Taxi {taxi_id} a {nueva_pos}")
            
            if taxis_activos[taxi_id]["destino"] == nueva_pos:
                pos_final = nueva_pos
                manejar_llegada_destino(taxis_activos,taxi_id)


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
            #taxis_activos[taxi_id]["cliente"] = solicitud["client_id"] #Hacemos que se introduzca en la base de datos cuando ya esté montado
            
            #Confirmar asignación al cliente
            print(f"Asignando taxi {taxi_id} al cliente {solicitud['client_id']}.")
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
    taxis_activos[taxi_id]["estado"] = "rojo"
    taxis_activos[taxi_id]["destino"] = (None, None)
    taxis_activos[taxi_id]["cliente"] = None
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
        client_id = solicitud["client_id"]
        ubicacion_cliente = solicitud["ubicacion_actual"]
        
        print(f"Solicitud de cliente recibida: {solicitud}")
        actualizar_mapa("cliente", solicitud["client_id"], solicitud["ubicacion_actual"], None)
        
        solicitudes_pendientes[client_id] = ubicacion_cliente
        
        asignar_taxi(solicitud, taxis_activos)

def manejar_llegada_destino(taxis_activos,taxi_id):
    global pos_final
    """Procesa la llegada de un taxi al destino del cliente."""
    destino = taxis_activos[taxi_id]["destino"]
    #RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
    # Notificar al cliente que el taxi ha llegado
    mensaje_cliente = {
        "client_id": taxis_activos[taxi_id]["cliente"],  
        "mensaje": f"Taxi {taxi_id} ha llegado a su destino {destino}",
        "ubicacion": pos_final
    }
    producer.send(TOPIC_CONFIRMATION, value=mensaje_cliente)
    producer.flush()

    liberar_taxi(taxi_id,taxis_activos)

#Usar en casos que cambiemos el estado de un taxi
def actualizar_base_datos(taxis_activos, file_path='bdd.txt'):
    with open(file_path, 'w') as f:
        for taxi_id, datos in taxis_activos.items():
            estado = "si" if datos["libre"] else "no"
            destino_x = datos["destino"][0] if datos["destino"][0] is not None else '-'
            destino_y = datos["destino"][1] if datos["destino"][1] is not None else '-'
            cliente = datos["cliente"].strip() if datos["cliente"] else '-'  # Usamos strip() para limpiar espacios extra
            f.write(f"{taxi_id}, {estado}, {datos['estado']}, {destino_x}, {destino_y}, {cliente}\n")
    print("Base de datos actualizada.")

EMPTY = "."

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

taxis_activos = leer_base_datos() # variable global para leer la base de datos

# Modificar la función main para iniciar el escucha de posición en paralelo
def main():
    pintar_mapa_inicial()  # Dibuja el mapa al inicio mostrando ubicaciones

    # Hilos para manejar conexiones de taxis y escuchar posiciones
    threading.Thread(target=iniciar_socket_taxi, args=(taxis_activos,)).start()
    threading.Thread(target=escuchar_peticiones_cliente, args=(taxis_activos,)).start()
    threading.Thread(target=escuchar_actualizaciones_taxi, args=(taxis_activos,)).start()

if __name__ == '__main__':
    main()