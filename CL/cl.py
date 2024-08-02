import logging
import json
import socket
import time
import threading
from multiprocessing import Process, Barrier
from typing import Final, Optional, Any, Iterator, List

data = [[("key1", "value1"), ("key2", "value2")], [("key3", "value3"), ("key4", "value4")]]

def setup_logging():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(process)d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

class Mapper(Process):
  def __init__(self, idx: int, downstream: List[int]):
    super().__init__()
    setup_logging()
    self.idx = idx - 1
    self.id: Final[str] = f"Mapper#{idx}"
    self.downstream = downstream
    self.setSocket()
    
  def setSocket(self):
    R1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    R1_socket.connect(("localhost", self.downstream[0]))
    self.R1_socket = R1_socket
    R2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    R2_socket.connect(("localhost", self.downstream[1]))
    self.R2_socket = R2_socket

  def send_data(self, socket, key, value):
    data = f"{key},{value},{self.id}".encode()
    length_prefix = f"{len(data):<10}".encode()
    socket.sendall(length_prefix + data)
  
  def run(self):
    count = 0
    howMuchToSend = 0
    try:
      while True:
        # here we will read file name from redis stream in an infinite loop
        # we will tokenise the content of the file and send (word, count) to corresponding reducer
        # use code from last years lab to do this
        key = "hello"
        value = 2
        self.send_data(self.R1_socket, key, value)
        key = "world"
        value = 3
        self.send_data(self.R2_socket, key, value)
        count = (count + 1) % 10
        if count == 0: # sending checkpoint markers after every 10 files
          self.send_data(self.R1_socket, "MARKER", -1)
          self.send_data(self.R2_socket, "MARKER", -1)
        howMuchToSend += 1
        if (howMuchToSend == 20):
          break
    except Exception as e:
        print(f"Error: {e}")
    finally:
      self.R1_socket.close()
      self.R2_socket.close()


class Reducer(Process):
  def __init__(self, idx: int, listenPort: int):
    super().__init__()
    setup_logging()
    self.listenPort = listenPort
    self.id: Final[str] = f"Reducer#{idx}"
    self.store: dict[str, int] = {}
    self.checkpoint_counter = 1  # checkpoint files will be named f"checkpoint/id_{checkpoint_counter}"
    self.markers = 0
    self.barrier = Barrier(2)
  
  def wordCount(self, key, value):
    if key in self.store:
      self.store[key] += value
    else:
      self.store[key] = value
  
  def checkpoint(self):
    filename = f"checkpoints/{self.id}_{self.checkpoint_counter}.txt"
    with open(filename, 'w') as file:
      json.dump(self.store, file)
    self.checkpoint_counter += 1
  
  def handle_client(self, client_socket, name, checkpointThread):
    try:
      while True:
          while True:
            length_prefix = client_socket.recv(10)
            if not length_prefix:
              break
            message_length = int(length_prefix.decode().strip())
            data = client_socket.recv(message_length).decode()
            key, value, id = data.split(',')
            value = int(value)
            logging.info(f"{name} has Received from {id}: Key={key}, Value={value}")
            # print(f"{name} has Received from {id}: Key={key}, Value={value}")
            if value != -1:  # normal (string, int) received
              self.wordCount(key, value)
            else: # received checkpoint marker
              # some complex logic here for making sure we have both markers
              # self.checkpoint()
              current_thread = threading.current_thread()
              print(f"[{current_thread.ident}] {name} has Received MARKER from {id}")
              self.barrier.wait()
              print("BOTH MARKERS RECEIVED")
              if checkpointThread == 1:
                self.checkpoint()

    except Exception as e:
        print(f"Error: {e}")

  def start_server(self, host, port, name):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    checkpointThread = 0
    logging.info(f"{name} listening on {host}:{port}")
    print(f"{name} listening on {host}:{port}")
    try:
      while True:
          client_socket, addr = server_socket.accept()
          logging.info(f"Accepted connection from {addr} for {name}")
          client_handler = threading.Thread(target=self.handle_client, args=(client_socket, name, checkpointThread))
          checkpointThread = 1 - checkpointThread
          client_handler.start()
    except Exception as e:
        print(f"Error: {e}")
    finally:
      server_socket.close()
    
  def run(self):
    self.start_server("localhost", self.listenPort, self.id)

class SetupMapReduce():
  def __init__(self) -> None:
    self.reducer_ports = [5023, 5024]
    self.startReducers()
    time.sleep(2)
    self.startMappers()

  def startMappers(self):
    M1 = Mapper(1, self.reducer_ports)
    M2 = Mapper(2, self.reducer_ports)
    M1.start()
    M2.start()

  def startReducers(self):
    R1 = Reducer(1, self.reducer_ports[0])
    R2 = Reducer(2, self.reducer_ports[1])
    R1.start()
    R2.start()

if __name__ == "__main__":
  setup_logging()
  SetupMapReduce()
