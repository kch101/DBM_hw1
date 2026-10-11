import json
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from redis_client import STREAM, ensure_group, master as r, replica, wait_for_redis

CACHE_TTL = 60
STREAM_RETENTION_MS = 7 * 24 * 60 * 60 * 1000  # 7 дней
LEADERBOARD = "tournament:main"
LOGIN_TTL = 24 * 60 * 60  # 24 часа

# INCR + EXPIRE одним атомарным действием: между ними никто не вклинится
login_script = r.register_script("""
local n = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return n
""")


def player_key(player_id: str) -> str:
    return f"player:{player_id}"


def cache_key(player_id: str) -> str:
    return f"cache:player:{player_id}"


def achievements_key(player_id: str) -> str:
    return f"achievements:{player_id}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    wait_for_redis()
    ensure_group()
    yield


app = FastAPI(title="GameHub", lifespan=lifespan)


@app.exception_handler(redis.ResponseError)
async def redis_response_error(request: Request, exc: redis.ResponseError):
    if str(exc).startswith("NOREPLICAS"):
        return JSONResponse(status_code=503, content={"detail": f"Write rejected by master: {exc}"})
    return JSONResponse(status_code=500, content={"detail": f"Redis error: {exc}"})


@app.exception_handler(redis.ConnectionError)
@app.exception_handler(redis.TimeoutError)
async def redis_unavailable(request: Request, exc: redis.RedisError):
    return JSONResponse(status_code=503, content={"detail": f"Redis unavailable: {exc}"})


class PlayerIn(BaseModel):
    name: str
    level: int = 1
    region: str


class BatchPlayer(PlayerIn):
    id: str


class BatchIn(BaseModel):
    players: list[BatchPlayer]


class LevelDelta(BaseModel):
    delta: int


class ScoreIn(BaseModel):
    player_id: str
    score: float


class AchievementIn(BaseModel):
    name: str


def player_mapping(player: PlayerIn) -> dict:
    return {
        "name": player.name,
        "level": player.level,
        "region": player.region,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/players/batch")
def create_players_batch(body: BatchIn):
    started = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    for player in body.players:
        pipe.hset(player_key(player.id), mapping=player_mapping(player))
        pipe.delete(cache_key(player.id))
    pipe.execute()
    elapsed_ms = (time.perf_counter() - started) * 1000
    return {"created": len(body.players), "elapsed_ms": round(elapsed_ms, 2)}


@app.post("/api/players/{player_id}")
def create_or_update_player(player_id: str, player: PlayerIn):
    r.hset(player_key(player_id), mapping=player_mapping(player)) # HSET player:{id} name.level.region..created_at
    r.delete(cache_key(player_id))
    return {"id": player_id, "status": "saved"}


@app.get("/api/players/{player_id}")
def get_player(player_id: str):
    cached = r.get(cache_key(player_id))
    if cached is not None:
        return {"cache": "hit", "profile": json.loads(cached)}

    profile = r.hgetall(player_key(player_id))
    if not profile:
        raise HTTPException(status_code=404, detail="Player not found")
    r.set(cache_key(player_id), json.dumps(profile), ex=CACHE_TTL)
    return {"cache": "miss", "profile": profile}


@app.patch("/api/players/{player_id}/level")
def update_level(player_id: str, body: LevelDelta):
    if not r.exists(player_key(player_id)):
        raise HTTPException(status_code=404, detail="Player not found")

    new_level = r.hincrby(player_key(player_id), "level", body.delta) #HINCRBY player:{id} level {delta}
    r.delete(cache_key(player_id)) # DEL cache:player:{id}
    now_ms = int(time.time() * 1000)
    r.xadd(
        STREAM,
        {
            "player_id": player_id,
            "type": "level_up",
            "message": f"Level changed by {body.delta}, now {new_level}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        minid=now_ms - STREAM_RETENTION_MS,
    )
    return {"id": player_id, "level": new_level}


@app.post("/api/players/{player_id}/login")
def login(player_id: str):
    count = login_script(keys=[f"logins:{player_id}"], args=[LOGIN_TTL])  #INCR + EXPIRE 86400
    return {"id": player_id, "logins_today": count}


@app.post("/api/leaderboard/score")
def add_score(body: ScoreIn):
    total = r.zincrby(LEADERBOARD, body.score, body.player_id)  #ZINCRBY tournament:main {score} {player_id}
    return {"player_id": body.player_id, "score": total}


@app.get("/api/leaderboard/top")
def leaderboard_top(limit: int = 10):
    top = replica.zrevrange(LEADERBOARD, 0, limit - 1, withscores=True)# ZREVRANGE tournament:main 0 9 WITHSCORES
    return [
        {"place": i + 1, "player_id": player_id, "score": score}
        for i, (player_id, score) in enumerate(top)
    ]


@app.get("/api/leaderboard/rank/{player_id}")
def leaderboard_rank(player_id: str):
    rank = replica.zrevrank(LEADERBOARD, player_id) #тут zrevrank тк именно он вернет 0 самому успешнмоу игроку (ну и потом +1 в ответе)
    if rank is None:
        raise HTTPException(status_code=404, detail="Player not in leaderboard")
    return {"player_id": player_id, "place": rank + 1, "score": replica.zscore(LEADERBOARD, player_id)}


@app.post("/api/players/{player_id}/achievements")
def add_achievement(player_id: str, body: AchievementIn):
    added = r.sadd(achievements_key(player_id), body.name)
    return {"id": player_id, "achievement": body.name, "added": bool(added)}


@app.get("/api/players/{player_id}/achievements/{name}")
def has_achievement(player_id: str, name: str):
    has = replica.sismember(achievements_key(player_id), name)
    return {"id": player_id, "achievement": name, "has": bool(has)}


@app.get("/api/players/{player_id}/achievements/common/{other_id}")
def common_achievements(player_id: str, other_id: str):
    common = replica.sinter(achievements_key(player_id), achievements_key(other_id))
    return {"players": [player_id, other_id], "common": sorted(common)}
