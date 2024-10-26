import socket
import time
import msvcrt
import json

def cargar_configuracion(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

config = cargar_configuracion('config.json')

CENTRAL_IP = config["central"]["ip"]
SENSOR_PORT = config["taxi"]["sensors_port"]
TAXI_ID = config["taxi"]["taxi_id"]

def conectar_con_taxi():
    """Establece la conexión con el taxi una vez y solo intenta reconectar si ocurre un error."""
    while True:
        try:
            taxi_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            taxi_socket.connect((CENTRAL_IP, SENSOR_PORT))
            taxi_socket.send(str(TAXI_ID).encode())
            print(f"Sensor del taxi {TAXI_ID} conectado.")
            listen_for_key_press(taxi_socket)
            break
        except (ConnectionRefusedError, ConnectionAbortedError):
            print("Taxi no disponible o conexión perdida. Reintentando en 2 segundos...")
            taxi_socket.close()
            time.sleep(2)
        except Exception as e:
            print(f"Error inesperado al conectar con el taxi: {e}")
            taxi_socket.close()
            break


def listen_for_key_press(taxi_socket):
    """Alterna entre OK y KO al presionar una tecla."""
    message = 'OK'
    while True:
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key:
                message = 'KO' if message == 'OK' else 'OK'
                print(f"Sensor del taxi {TAXI_ID} cambió el estado a {message}")
                try:
                    taxi_socket.sendall(message.encode())
                except (ConnectionResetError, ConnectionAbortedError):
                    print("Error al enviar mensaje al taxi. Conexión cerrada.")
                    break
                except Exception as e:
                    print(f"Error al enviar mensaje al taxi: {e}")
                    break
        time.sleep(1)

if __name__ == "__main__":
    conectar_con_taxi()
