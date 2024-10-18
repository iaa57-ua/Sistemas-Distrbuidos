import socket
import threading
import time
import keyboard  # Importa el módulo para detectar teclas

class Sensor:
    def __init__(self, digital_engine_ip, digital_engine_port):
        self.digital_engine_ip = digital_engine_ip
        self.digital_engine_port = digital_engine_port
        self.message = 'OK'
        self.running = True

    def connect_to_digital_engine(self):
        self.de_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET NOS DICE QUE ES IPV4 Y SHOCK_STREAM TCP
        self.de_socket.connect((self.digital_engine_ip, self.digital_engine_port))

    def send_sensor_data(self):
        while self.running:
            self.de_socket.sendall(self.message.encode())
            time.sleep(1)

    def change_message(self):
        while self.running:
            if keyboard.is_pressed('space'):  # Detecta si el espacio está presionada
                self.message = 'KO'
            else:
                self.message = 'OK'
            time.sleep(0.1)  # Controlar la frecuencia de comprobación

    def run(self):
        self.connect_to_digital_engine()
        threading.Thread(target=self.send_sensor_data).start()
        threading.Thread(target=self.change_message).start()

if __name__ == "__main__":
    sensor = Sensor('127.0.0.1', 12346)
    sensor.run()
