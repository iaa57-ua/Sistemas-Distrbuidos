import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

def main():
    if len(sys.argv) == 5:
        SERVER = sys.argv
        PORT = int(sys.argv)
        ADDR = (SERVER, PORT)
        TAXI_ID = sys.argv
        SENSOR_PORT = int(sys.argv)
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print(f"Conectado a la central en [{ADDR}] como Taxi ID {TAXI_ID}")

        msg = f"AUTH {TAXI_ID}"
        send(msg, client)
        response = client.recv(2048).decode(FORMAT)
        print(f"Respuesta de la central: {response}")

        while True:
            msg = input("Ingrese comando para el taxi: ")
            if msg == FIN:
                break
            send(msg, client)
            response = client.recv(2048).decode(FORMAT)
            print(f"Respuesta de la central: {response}")

        print("Finalizando conexión")
        send(FIN, client)
        client.close()
    else:
        print("Uso: <ServerIP> <Puerto> <TaxiID> <SensorPort>")

if __name__ == "__main__":
    main()

