import socket
import threading
import time
import keyboard
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
DIGITAL_ENGINE_IP = config["central_ip"]
DIGITAL_ENGINE_PORT = config["sensors_port"]

class Sensor:
    def __init__(self, digital_engine_ip, digital_engine_port):
        self.digital_engine_ip = digital_engine_ip
        self.digital_engine_port = digital_engine_port
        self.message = 'OK'
        self.running = True

    def connect_to_digital_engine(self):
        self.de_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.de_socket.connect((self.digital_engine_ip, self.digital_engine_port))

    def send_sensor_data(self):
        while self.running:
            self.de_socket.sendall(self.message.encode())
            time.sleep(1)

    def change_message(self):
        while self.running:
            if keyboard.is_pressed('space'):
                self.message = 'KO'
            else:
                self.message = 'OK'
            time.sleep(0.1)

    def run(self):
        self.connect_to_digital_engine()
        threading.Thread(target=self.send_sensor_data).start()
        threading.Thread(target=self.change_message).start()

if __name__ == "__main__":
    sensor = Sensor(DIGITAL_ENGINE_IP, DIGITAL_ENGINE_PORT)
    sensor.run()
