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
DIGITAL_ENGINE_IP = config["central"]["ip"]
TAXI_ID = config["taxi"]["taxi_id"]
DIGITAL_ENGINE_PORT = config["taxi"]["sensors_port"]

class Sensor:
    def __init__(self, digital_engine_ip, digital_engine_port, taxi_id):
        self.digital_engine_ip = digital_engine_ip
        self.digital_engine_port = digital_engine_port
        self.message = 'OK'
        self.running = True
        self.taxi_id = taxi_id  # Asociar el sensor con un ID de taxi
        
    def send_ready_message(self):
        """Envía un mensaje al taxi indicando que el sensor está conectado y listo."""
        self.de_socket.sendall(f"READY {self.taxi_id}".encode())


    def connect_to_digital_engine(self):
        self.de_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.de_socket.connect((self.digital_engine_ip, self.digital_engine_port))

    def send_sensor_data(self):
        while self.running:
            # Envía el estado del sensor al Digital Engine
            self.de_socket.sendall(self.message.encode())
            time.sleep(1)

    def listen_for_key_press(self):
        """Captura la pulsación de una tecla en la terminal actual para parar o reanudar el taxi."""
        print(f"Sensor del taxi {self.taxi_id} está esperando la tecla para cambiar el estado (OK/KO)...")
        while self.running:
            if msvcrt.kbhit():  # Si se ha pulsado una tecla
                key = msvcrt.getch()  # Captura la tecla (no necesita ser específica)
                if key:  # Cuando se presiona cualquier tecla
                    # Alternar entre 'KO' y 'OK'
                    self.message = 'KO' if self.message == 'OK' else 'OK'
                    print(f"Sensor del taxi {self.taxi_id}: Cambió el estado a {self.message}")

    def run(self):
        self.connect_to_digital_engine()
        self.send_ready_message()  # Notificar que el sensor está listo
        threading.Thread(target=self.send_sensor_data).start()
        self.listen_for_key_press()  # No necesita ser en un hilo separado


if __name__ == "__main__":
    # Mostrar el mensaje al iniciar el sensor
    print(f"Iniciando sensor del taxi {TAXI_ID} en el puerto {DIGITAL_ENGINE_PORT}...")

    sensor = Sensor(DIGITAL_ENGINE_IP, DIGITAL_ENGINE_PORT, TAXI_ID)
    sensor.run()
