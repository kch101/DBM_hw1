import time
from datetime import datetime, timezone

from redis_client import master as r, wait_for_redis

COUNT = 20
REGIONS = ["EU", "NA", "ASIA"]


def players(prefix: str):
    for i in range(1, COUNT + 1):
        yield f"{prefix}{i}", {
            "name": f"Player {i}",
            "level": i % 10 + 1,
            "region": REGIONS[i % len(REGIONS)],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }


def without_pipeline() -> float:
    started = time.perf_counter()
    for player_id, mapping in players("seq"):
        r.hset(f"player:{player_id}", mapping=mapping)
    return (time.perf_counter() - started) * 1000


def with_pipeline() -> float:
    started = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    for player_id, mapping in players("bulk"):
        pipe.hset(f"player:{player_id}", mapping=mapping)
    pipe.execute()
    return (time.perf_counter() - started) * 1000


if __name__ == "__main__":
    wait_for_redis()
    seq_ms = without_pipeline()
    pipe_ms = with_pipeline()
    print(f"{COUNT} profiles one by one:   {seq_ms:.2f} ms")
    print(f"{COUNT} profiles via pipeline: {pipe_ms:.2f} ms")
    print(f"Speedup: x{seq_ms / pipe_ms:.1f}")
