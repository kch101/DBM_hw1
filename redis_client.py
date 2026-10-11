import os
import time

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel

SENTINELS = [
    (host, int(port))
    for host, port in (
        addr.split(":")
        for addr in os.getenv(
            "REDIS_SENTINELS",
            "redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379",
        ).split(",")
    )
]
MASTER_NAME = os.getenv("REDIS_MASTER_NAME", "mymaster")

STREAM = "notifications"
GROUP = "notifications-group"

_connection_kwargs = {
    "decode_responses": True,
    "socket_timeout": 5,
    "socket_connect_timeout": 5,
    "retry": Retry(ExponentialBackoff(cap=2, base=0.2), 10),
    "retry_on_error": [redis.ConnectionError, redis.TimeoutError],
}

sentinel = Sentinel(
    SENTINELS,
    sentinel_kwargs={"socket_timeout": 2, "socket_connect_timeout": 2},
)
master = sentinel.master_for(MASTER_NAME, **_connection_kwargs)
replica = sentinel.slave_for(MASTER_NAME, **_connection_kwargs)


def wait_for_redis(attempts: int = 30, delay: float = 1.0) -> None:
    for attempt in range(1, attempts + 1):
        try:
            master.ping()
            print(f"Connected to Redis master {sentinel.discover_master(MASTER_NAME)}", flush=True)
            return
        except redis.RedisError as e:
            print(f"Redis is not ready ({attempt}/{attempts}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("Redis is unavailable")


def ensure_group(attempts: int = 30, delay: float = 1.0) -> None:
    # XGROUP CREATE — запись: пока реплики не подключились, мастер отвечает NOREPLICAS
    for attempt in range(1, attempts + 1):
        try:
            master.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
            return
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                return
            if not str(e).startswith("NOREPLICAS") or attempt == attempts:
                raise
            print(f"Master has no replicas yet ({attempt}/{attempts})", flush=True)
            time.sleep(delay)
