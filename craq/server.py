import logging
from concurrent import futures
from itertools import tee
from multiprocessing import Process
from time import sleep
from typing import Final, Optional, Any, Iterator

import grpc

from kvstore_pb2 import GetRequest, GetResponse, SetRequest, SetResponse
from kvstore_pb2_grpc import KVStore, KVStoreStub, add_KVStoreServicer_to_server

def setup_logging():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(process)d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')


class CRAQ(KVStore):
  def __init__(self, id: str, nxt_port: Optional[int] = None):
    setup_logging()
    self.id = id
    self.store: dict[str, str] = {}
    self.nxt_stub = None
    if nxt_port:
      channel = grpc.insecure_channel(f"localhost:{nxt_port}")
      self.nxt_stub = KVStoreStub(channel)

  def GetValue(self, request_iterator: Iterator[GetRequest], *args: Any, **kwargs: Any):
    try:
      for request in request_iterator:
        if request.key in self.store:
          val = self.store[request.key]
          yield GetResponse(value=val, status=True, request_id=request.id)
        else:
          logging.warning(f"{request.key} not found by Store {self.id}")
          yield GetResponse(status=False, request_id=request.id)
    except Exception as e:
      logging.error(f"Exception in GetValue: {e}")
      raise

  def SetValue(self, request_iterator: Iterator[SetRequest], *args: Any, **kwargs: Any):
    logging.info(f"{self.id} got SetValue request")
    try:
      req_iter1, req_iter2 = tee(request_iterator)
      for request in req_iter1:
        logging.info(f"{self.id} setting key={request.key}")
        self.store[request.key] = request.value

      if self.nxt_stub:
        logging.info(f"{self.id} forwarding requests")
        # Forward writes to next server in the chain
        responses = self.nxt_stub.SetValue(req_iter2)
        # TODO: call does not even happen if we don't read the responses! Figure out how to make this eager.
        for response in responses:
          yield response
      else:
        logging.info(f"{self.id} is tail. yielding responses")
        # I am tail
        for request in req_iter2:
          yield SetResponse(status=True, request_id=request.id)
    except Exception as e:
      logging.error(f"Exception in SetValue: {e}")
      raise


class Server(Process):
  def __init__(self, idx: int, port: int, nxt_port: Optional[int]):
    super().__init__()
    setup_logging()
    self.id: Final[str] = f"Server#{idx}"
    self.port = port
    self.nxt_port = nxt_port

  def run(self):
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_KVStoreServicer_to_server(CRAQ(self.id, self.nxt_port), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{self.port}")
    grpc_server.start()
    logging.info(f"Server process with pid {self.pid} started, listening on {self.port}")
    grpc_server.wait_for_termination()


ports = [50051, 50052, 50053]
# ports = [50051]

class Connection:
  def __init__(self):
    self.request_id = 0
    self.head_channel = grpc.insecure_channel(f"localhost:{ports[0]}")
    self.head_stub = KVStoreStub(self.head_channel)

    self.tail_channel = grpc.insecure_channel(f"localhost:{ports[-1]}")
    self.tail_stub = KVStoreStub(self.tail_channel)

  def close(self):
    self.head_channel.close()
    self.tail_channel.close()

  def get(self, key: str) -> str:
    responses = self.tail_stub.GetValue(iter([GetRequest(key=key, id=self.request_id)]))
    self.request_id += 1

    for response in responses:
      # response = head_stub.GetValue(GetRequest(key="Test"))
      logging.info(
        f"For GET, Client received: status={response.status} and value={response.value} for request: {response.request_id}")
      if response.request_id == self.request_id - 1:
        return response.value
    return ""

  def set(self, key: str, value: str) -> None:
    responses = self.head_stub.SetValue(iter([SetRequest(key=key, value=value, id=self.request_id)]))
    self.request_id += 1

    for response in responses:
      logging.info(f"For SET, Client received: status={response.status} for request: {response.request_id}")

def serve():
  for i, port in enumerate(ports):
    s = Server(idx=i, port=port, nxt_port=ports[i + 1] if i + 1 < len(ports) else None)
    s.start()


def client():
  con = Connection()
  con.set(key="hello", value="world")
  v = con.get(key="hello")

if __name__ == "__main__":
  setup_logging()
  serve()
  sleep(1)  # wait for servers to start
  client()
