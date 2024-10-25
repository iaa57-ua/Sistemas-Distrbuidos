import socket
import threading
import time
import msvcrt  # Librería para capturar teclas en Windows
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

# Usar parámetros de la configuración
CENTRAL_IP = config["central"]["ip"]
CENTRAL_PORT = config["central"]["port"]
TAXI_ID = config["taxi"]["taxi_id"]
SENSOR_PORT = config["taxi"]["sensors_port"]

class Sensor:
    def __init__(self, central_ip, central_port, sensor_port, taxi_id):
        self.central_ip = central_ip
        self.central_port = central_port
        self.sensor_port = sensor_port
        self.message = 'OK'
        self.running = True
        self.taxi_id = taxi_id
        self.central_socket = None
        self.taxi_socket = None

    def conectar_con_central(self):
        """Conectar el sensor con la Central y esperar a que esté disponible."""
        while self.central_socket is None:
            try:
                self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.central_socket.connect((self.central_ip, self.central_port))
                print(f"Sensor conectado con la Central en {self.central_ip}:{self.central_port}")
                self.central_socket.send(f"READY {self.taxi_id}".encode())
            except ConnectionRefusedError:
                print("Central no disponible, reintentando en 2 segundos...")
                time.sleep(2)
                self.central_socket = None

    def conectar_con_taxi(self):
        """Conectar el sensor con el Taxi y esperar a que esté disponible."""
        while self.taxi_socket is None:
            try:
                self.taxi_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.taxi_socket.connect((self.central_ip, self.sensor_port))
                print(f"Sensor conectado con el Taxi en {self.central_ip}:{self.sensor_port}")
                # Enviar mensaje "READY" para indicar que el sensor está listo
                self.taxi_socket.send("READY".encode())
            except ConnectionRefusedError:
                print("Taxi no disponible, reintentando en 2 segundos...")
                time.sleep(2)
                self.taxi_socket = None

    def send_sensor_data(self):
        """Enviar datos periódicos del sensor al Taxi."""
        while self.running:
            self.taxi_socket.sendall(self.message.encode())
            time.sleep(1)

    def listen_for_key_press(self):
        """Captura la pulsación de una tecla para cambiar el estado del sensor entre 'OK' y 'KO'."""
        print(f"Sensor del taxi {self.taxi_id} esperando...")
        while self.running:
            if msvcrt.kbhit():
                key = msvcrt.getch()
                if key:
                    self.message = 'KO' if self.message == 'OK' else 'OK'
                    print(f"Sensor del taxi {self.taxi_id} cambió el estado a {self.message}")

    def escuchar_comandos_central(self):
        """Escucha los comandos desde la Central y los envía al Taxi."""
        while self.running:
            try:
                data = self.central_socket.recv(1024).decode()
                if data:
                    comando = json.loads(data)
                    print(f"Recibido comando de la Central: {comando}")
                    self.taxi_socket.send(json.dumps(comando).encode())
                    print(f"Comando enviado al Taxi: {comando}")
            except Exception as e:
                print(f"Error al recibir datos de la Central: {e}")
                break

    def run(self):
        """Ejecuta el proceso del sensor."""
        # Paso 1: Conectar con la Central
        self.conectar_con_central()
        # Paso 2: Conectar con el Taxi
        self.conectar_con_taxi()
        # Paso 3: Iniciar la comunicación con la Central y el Taxi
        threading.Thread(target=self.send_sensor_data).start()
        threading.Thread(target=self.listen_for_key_press).start()
        self.escuchar_comandos_central()

if __name__ == "__main__":
    # Mostrar el mensaje al iniciar el sensor
    print(f"Iniciando sensor del taxi {TAXI_ID}...")

    sensor = Sensor(CENTRAL_IP, CENTRAL_PORT, SENSOR_PORT, TAXI_ID)
    sensor.run()
