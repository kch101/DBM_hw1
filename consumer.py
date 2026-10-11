import os
import time

import redis

from redis_client import GROUP, STREAM, ensure_group, master as r, wait_for_redis

CONSUMER = os.getenv("CONSUMER_NAME", "consumer-1")
BLOCK_MS = 2000


def handle(msg_id: str, fields: dict) -> None:
    print(
        f"[{fields.get('timestamp')}] player={fields.get('player_id')} "
        f"type={fields.get('type')} message={fields.get('message')!r} id={msg_id}",
        flush=True,
    )


def read_batch(start_id: str, block: int | None) -> int:
    response = r.xreadgroup(GROUP, CONSUMER, {STREAM: start_id}, count=10, block=block)
    processed = 0
    for _, messages in response or []:
        for msg_id, fields in messages:
            if fields:
                handle(msg_id, fields)
            r.xack(STREAM, GROUP, msg_id)
            print(f"XACK {msg_id}", flush=True)
            processed += 1
    return processed


def main() -> None:
    wait_for_redis()
    ensure_group()
    print(f"Consumer {CONSUMER} listening on {STREAM} ({GROUP})", flush=True)
    check_pending = True
    while True:
        try:
            if check_pending:
                check_pending = read_batch("0", None) > 0
            else:
                read_batch(">", BLOCK_MS)
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                ensure_group()
            else:
                print(f"Redis error: {e}", flush=True)
                time.sleep(1)
        except (redis.ConnectionError, redis.TimeoutError) as e:
            print(f"Redis unavailable, retrying: {e}", flush=True)
            check_pending = True
            time.sleep(1)


if __name__ == "__main__":
    main()
