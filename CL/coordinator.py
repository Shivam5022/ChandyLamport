import glob
import logging
import os
import signal
import sys
import time
from threading import current_thread

from mrds import MyRedis
from chandyLamport import SetupMapReduce

if __name__ == "__main__":
  start_time = time.time()
  # logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, force=True, format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s')
  thread = current_thread()
  thread.name = "client"

  rds = MyRedis()

  pattern = 'csv_files/*.csv'
  for file in glob.glob(pattern):
    rds.add_file(file)  

  SetupMapReduce()

  

  

