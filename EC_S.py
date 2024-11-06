import socket
import time
import msvcrt
import json
import threading

def cargar_configuracion(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

config = cargar_configuracion('config.json')

TAXI_IP = config["taxi"]["taxi_ip"]
SENSOR_PORT = config["taxi"]["sensors_port"]
TAXI_ID = config["taxi"]["taxi_id"]
MENSAJE = "OK"

def conectar_con_taxi():
    """Establece la conexión con el taxi una vez y solo intenta reconectar si ocurre un error."""
    while True:
        try:
            taxi_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            taxi_socket.connect((TAXI_IP, SENSOR_PORT))
            taxi_socket.send(str(TAXI_ID).encode())
            print(f"Sensor del taxi {TAXI_ID} conectado.")
            return taxi_socket  # Retorna el socket si la conexión es exitosa
        
        except (ConnectionRefusedError, ConnectionAbortedError):
            print("Taxi no disponible o conexión perdida. Reintentando en 2 segundos...")
            taxi_socket.close()
            time.sleep(2)
        except Exception as e:
            print(f"Error inesperado al conectar con el taxi: {e}")
            taxi_socket.close()
            break
        
    return None

def listen_for_key_press(taxi_socket):
    """Cambia el estado entre 'OK' y 'KO' al presionar una tecla."""
    global MENSAJE
    message = 'OK'
    while True:
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key:
                message = 'KO' if message == 'OK' else 'OK'
                MENSAJE = message  # Actualizar el mensaje global
                print(f"Sensor del taxi {TAXI_ID} cambió el estado a {message}")
                try:
                    taxi_socket.sendall(message.encode())
                except (ConnectionResetError, ConnectionAbortedError):
                    print("Error al enviar mensaje al taxi. Conexión cerrada.")
                    break
                except Exception as e:
                    print(f"Error al enviar mensaje al taxi: {e}")
                    break
        time.sleep(0.1)  # Reducir el tiempo de espera para detectar cambios de tecla rápidamente

def mostrar_estado(taxi_socket):
    """Muestra el estado actual del sensor y lo envía al Digital Engine cada segundo."""
    global MENSAJE
    while True:
        print(f"Estado actual del taxi {TAXI_ID}: {MENSAJE}")
        
        # Enviar el estado actual al Digital Engine
        try:
            taxi_socket.sendall(MENSAJE.encode())
        except (ConnectionResetError, ConnectionAbortedError):
            print("Error al enviar estado al Digital Engine. Conexión cerrada.")
            break
        except Exception as e:
            print(f"Error al enviar estado al Digital Engine: {e}")
            break

        time.sleep(1)  # Enviar el estado cada segundo

def main():
    taxi_socket = conectar_con_taxi()
    if taxi_socket:
        # Crear los hilos correctamente pasando la referencia de las funciones sin llamarlas
        threadListenKey = threading.Thread(target=listen_for_key_press, args=(taxi_socket,))
        threadListenKey.start()
        
        threadMostrarEstado = threading.Thread(target=mostrar_estado, args=(taxi_socket,))
        threadMostrarEstado.start()

if __name__ == "__main__":
    main()