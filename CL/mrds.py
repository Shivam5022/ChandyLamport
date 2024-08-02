from __future__ import annotations

import logging
from typing import Optional, Final
import redis

GROUP: Final = "worker"
IN: Final[bytes] = b"files"
FNAME: Final[bytes] = b"fname"

class MyRedis:
  def __init__(self):
    self.rds: Final = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    # self.rds: Final = Redis(host='localhost', port=6379, password='pass', db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, GROUP, id="0", mkstream=True)

  def add_file(self, fname: str):
    self.rds.xadd(IN, {FNAME: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    COUNT: Final[bytes] = b"count"
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n, withscores=True)