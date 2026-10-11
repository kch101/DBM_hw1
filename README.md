# GameHub — Redis

Бэкенд игровой платформы: профили игроков, лидерборд турнира, достижения, счётчик входов и уведомления. Redis используется как хранилище и кеш, работает в отказоустойчивой конфигурации (мастер + 2 реплики + 3 Sentinel).

## Архитектура

```
                 ┌──────────────┐     ┌──────────────────┐
  HTTP :8000 ──▶ │  app (FastAPI)│     │ consumer (Stream) │
                 └──────┬───────┘     └────────┬─────────┘
                        │ Sentinel: где мастер / реплики?
          ┌─────────────┼──────────────────────┤
          ▼             ▼                      ▼
   sentinel-1:26379  sentinel-2:26380  sentinel-3:26381   (кворум 2)
          │ мониторинг
          ▼
   redis-master:6379  ──репликация──▶  redis-replica-1:6380
     (запись)                      └▶  redis-replica-2:6381  (чтение)

   redis-insight:5540 (мониторинг)
```

| Компонент | Контейнер | Порт на хосте |
|---|---|---|
| Мастер (RDB + AOF, `volatile-lru`, `maxmemory 256mb`, `min-replicas-to-write 1`) | `redis-master` | 6379 |
| Реплики (`replicaof redis-master 6379`, `replica-read-only yes`) | `redis-replica-1`, `redis-replica-2` | 6380, 6381 |
| Sentinel (`quorum 2`, `down-after-milliseconds 5000`, `failover-timeout 10000`) | `redis-sentinel-1..3` | 26379, 26380, 26381 |
| REST API | `gamehub-app` | 8000 |
| Потребитель уведомлений | `gamehub-consumer` | — |
| Redis Insight | `redis-insight` | 5540 |

Приложение подключается к Redis только через Sentinel (`redis.sentinel.Sentinel`): запись идёт в мастер (`master_for`), чтение лидерборда и достижений — с реплик (`slave_for`). Профили и кеш читаются с мастера, чтобы сразу после записи не получить устаревшие данные из реплики. Таймауты — 5 с, ретраи — экспоненциальный backoff, до 10 попыток; при старте приложение ждёт, пока Redis станет доступен.

Приложение и потребитель запускаются в той же Docker-сети, потому что Sentinel отдаёт адреса по именам контейнеров (`redis-master`, `redis-replica-1`), которые резолвятся только внутри сети.

## Структура проекта

```
docker-compose.yml     вся инфраструктура
redis/master.conf      конфиг мастера
redis/replica.conf     конфиг реплик
sentinel/sentinel.conf конфиг Sentinel
redis_client.py        подключение через Sentinel, ожидание Redis, создание группы потребителей
main.py                REST API (FastAPI)
consumer.py            потребитель потока notifications (XREADGROUP + XACK)
seed.py                массовая загрузка 20 профилей: без pipeline vs pipeline, замер времени
```

## Запуск

```bash
docker compose up -d --build
docker compose ps
```

Swagger: http://localhost:8000/docs  
Redis Insight: http://localhost:5540 (добавить базу `redis-master:6379`)

Проверка репликации:

```bash
docker exec -it redis-master redis-cli INFO replication     # connected_slaves:2
docker exec -it redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```

Остановка: `docker compose down` (данные остаются в volume), полная очистка: `docker compose down -v`.

## Структуры данных

| Данные | Ключ | Тип | TTL |
|---|---|---|---|
| Профиль игрока | `player:{id}` — `name`, `level`, `region`, `created_at` | Hash | ∞ |
| Счётчик входов | `logins:{id}` | String | 24 часа |
| Кеш профиля | `cache:player:{id}` — JSON профиля | String | 60 с |
| Лидерборд | `tournament:main` — member = `player_id`, score = очки | Sorted Set | ∞ |
| Достижения | `achievements:{id}` | Set | ∞ |
| Уведомления | `notifications` — `player_id`, `type`, `message`, `timestamp`; группа `notifications-group` | Stream | 7 дней (`XADD ... MINID`) |

## API

| Метод | Путь | Тело | Команды Redis |
|---|---|---|---|
| POST | `/api/players/{id}` | `{"name": "Timur", "level": 1, "region": "EU"}` | `HSET player:{id}`, `DEL cache:player:{id}` |
| GET | `/api/players/{id}` | — | `GET cache:player:{id}`; при промахе `HGETALL` + `SET ... EX 60`. В ответе `cache: hit/miss` |
| PATCH | `/api/players/{id}/level` | `{"delta": 2}` | `HINCRBY`, `DEL cache:player:{id}`, `XADD notifications MINID ~ <7 дней> * ...` |
| POST | `/api/players/{id}/login` | — | Lua: `INCR logins:{id}` + `EXPIRE 86400` атомарно |
| POST | `/api/players/batch` | `{"players": [{"id": "1", "name": "...", "level": 1, "region": "EU"}, ...]}` | pipeline из `HSET`, в ответе `elapsed_ms` |
| POST | `/api/leaderboard/score` | `{"player_id": "1", "score": 100}` | `ZINCRBY tournament:main` |
| GET | `/api/leaderboard/top?limit=10` | — | `ZREVRANGE tournament:main 0 9 WITHSCORES` |
| GET | `/api/leaderboard/rank/{id}` | — | `ZREVRANK` (место с 1, первое — у лидера) + `ZSCORE` |
| POST | `/api/players/{id}/achievements` | `{"name": "first_blood"}` | `SADD achievements:{id}` |
| GET | `/api/players/{id}/achievements/{name}` | — | `SISMEMBER achievements:{id} {name}` |
| GET | `/api/players/{id1}/achievements/common/{id2}` | — | `SINTER achievements:{id1} achievements:{id2}` |

Если мастер отклоняет запись (`NOREPLICAS`) или Redis недоступен, API отвечает `503` с текстом ошибки.

## Демонстрация

**Кеширование**

```bash
curl -X POST localhost:8000/api/players/1 -H 'content-type: application/json' -d '{"name":"Timur","level":1,"region":"EU"}'
curl localhost:8000/api/players/1        # "cache": "miss"
curl localhost:8000/api/players/1        # "cache": "hit"
docker exec -it redis-master redis-cli TTL cache:player:1
```

**Уведомления**

```bash
docker compose logs -f consumer          # в отдельном окне
curl -X PATCH localhost:8000/api/players/1/level -H 'content-type: application/json' -d '{"delta":2}'
docker exec -it redis-master redis-cli XLEN notifications
docker exec -it redis-master redis-cli XPENDING notifications notifications-group   # 0 — всё подтверждено
```

**Конвейеризация**

```bash
docker compose exec app python seed.py
```

**Отказоустойчивость (Sentinel)**

```bash
docker stop redis-master
sleep 15
docker exec -it redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster   # новый мастер
curl -X PATCH localhost:8000/api/players/1/level -H 'content-type: application/json' -d '{"delta":1}'
docker start redis-master                # вернётся как реплика нового мастера
```

**Защита от split-brain**

Выполнять на текущем мастере (после failover это может быть реплика — смотри `get-master-addr-by-name`):

```bash
docker pause redis-replica-1 redis-replica-2
sleep 12                                 # min-replicas-max-lag 10
docker exec -it redis-master redis-cli SET test 1   # (error) NOREPLICAS Not enough good replicas to write.
docker unpause redis-replica-1 redis-replica-2
```

Failover при этом не начинается: мастер жив, он сам перестаёт принимать записи, потому что не видит ни одной реплики.
