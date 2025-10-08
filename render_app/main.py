# telewebhook/main.py
# 단일 FastAPI 서비스:
# - 기존 API(/, /youtube, /chartdata/{category}, /market-holidays, /daily-saved-data, /test-save, /test-code)
# - Telegram 토큰리스 웹훅(/telegram/webhook/{name}/{secret})
# - 로컬 봇 WS 엔드포인트(/ws/{bot_id})
# - 공통 CORS/로깅/ENV 파싱

import os
import json
import time
import uuid
import asyncio
import logging
from collections import defaultdict, deque
from typing import Dict, Any, Optional, Set, Deque, Tuple

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, HTTPException, Header, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

# ─────────────────────────────────────────────────────────────────────
# 앱 생성
app = FastAPI()

# CORS (필요 시 도메인 제한하세요)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────
# 로깅 (logfmt style)
logger = logging.getLogger("telewebhook")
_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, _level, logging.INFO))
_h = logging.StreamHandler()
_h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_h)

def kv(**fields) -> str:
    return " ".join(f"{k}={repr(v)}" for k, v in fields.items() if v is not None)

# ─────────────────────────────────────────────────────────────────────
# ENV (no Telegram bot tokens needed)
BOTS_JSON = os.getenv("BOTS_JSON", "[]")
BOT_RESPONSE_TIMEOUT_SEC = int(os.getenv("BOT_RESPONSE_TIMEOUT_SEC", "8"))

# ─────────────────────────────────────────────────────────────────────
# Helpers for robust env parsing
def _strip_quotes(v: Optional[str]) -> str:
    if not v:
        return ""
    s = v.strip()
    if (s.startswith("'''") and s.endswith("'''")) or (s.startswith('"""') and s.endswith('"""')):
        return s[3:-3].strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1].strip()
    return s

# ─────────────────────────────────────────────────────────────────────
# Parse BOTS (no token needed)
# Each bot entry requires: name, path_secret
# Optional: header_secret (X-Telegram-Bot-Api-Secret-Token), stale_seconds, allowed_chats, ws_bot_id, ws_token
try:
    RAW_BOTS = json.loads(_strip_quotes(BOTS_JSON))
except Exception as e:
    raise RuntimeError(f"BOTS_JSON parse error: {e}")

BOTS: Dict[str, Dict[str, Any]] = {}
for b in RAW_BOTS:
    if not b.get("name") or not b.get("path_secret"):
        raise RuntimeError("Each bot requires name and path_secret.")
    BOTS[b["name"]] = {
        "path_secret": b["path_secret"],
        "header_secret": b.get("header_secret") or "",
        "stale_seconds": int(b.get("stale_seconds") or 60),
        "allowed_chats": b.get("allowed_chats") or [],
        "ws_bot_id": (b.get("ws_bot_id") or b["name"]),
        "ws_token": b.get("ws_token"),
    }

# Build WS tokens from BOTS_JSON
def _coerce_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

TOKENS: Dict[str, Set[str]] = {}
for name, cfg in BOTS.items():
    ws_id = (cfg.get("ws_bot_id") or name).strip()
    ws_tokens = {str(t).strip() for t in _coerce_list(cfg.get("ws_token")) if str(t).strip()}
    if ws_id and ws_tokens:
        TOKENS.setdefault(ws_id, set()).update(ws_tokens)

def _expected_tokens_for(bot_id: str) -> Set[str]:
    return TOKENS.get(bot_id, set())

# ─────────────────────────────────────────────────────────────────────
# 로컬 WS 레지스트리
# bot_id -> {"ws": WebSocket, "caps": set([...]), "waiters": {corr_id: {"future": fut}}}
bots_ws: Dict[str, Dict[str, Any]] = {}

def extract_msg(update: Dict[str, Any]) -> Dict[str, Any]:
    return update.get("message") or {}

_seen_ids: Dict[str, Set[int]] = defaultdict(set)          # botname -> seen update_ids
_seen_qs: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=2000))

def mark_seen(botname: str, uid: Optional[int]) -> bool:
    if uid is None:
        return True
    if uid in _seen_ids[botname]:
        return False
    _seen_ids[botname].add(uid)
    _seen_qs[botname].append(uid)
    return True

def is_stale(botname: str, update: Dict[str, Any]) -> Tuple[bool, int]:
    msg = extract_msg(update)
    ts = msg.get("date")
    if not ts:
        return (False, 0)
    age = int(time.time() - int(ts))
    return (age > BOTS[botname]["stale_seconds"], age)

def parse_command(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    return {"type": "TEXT_COMMAND", "raw_text": text}

def choose_bot_for(cmd: Dict[str, Any], tg_bot_name: Optional[str] = None) -> Optional[str]:
    need = cmd["type"]
    # 1) explicit mapping via ws_bot_id if connected
    if tg_bot_name:
        mapped = BOTS.get(tg_bot_name, {}).get("ws_bot_id")
        if mapped and mapped in bots_ws:
            if need in bots_ws[mapped].get("caps", set()):
                return mapped
    # 2) fallback: first online bot with capability
    for bot_id, info in bots_ws.items():
        if need in info.get("caps", set()):
            return bot_id
    return None

# ─────────────────────────────────────────────────────────────────────
# 상태 체크
@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "telegram_bots": list(BOTS.keys()),
        "local_ws_bots": list(bots_ws.keys()),
        "seen": {k: len(v) for k, v in _seen_ids.items()},
    }

# ─────────────────────────────────────────────────────────────────────
# Telegram 토큰리스 웹훅
@app.post("/telegram/webhook/{name}/{secret}")
async def telegram_webhook(
    name: str,
    secret: str,
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # 1) validate bot + secrets
    if name not in BOTS:
        raise HTTPException(status_code=404)
    cfg = BOTS[name]
    if secret != cfg["path_secret"]:
        raise HTTPException(status_code=404)
    if cfg["header_secret"]:
        if not x_telegram_bot_api_secret_token or x_telegram_bot_api_secret_token != cfg["header_secret"]:
            raise HTTPException(status_code =403, detail="Bad secret header")

    # 2) parse update & dedup/TTL
    update = await req.json()
    uid = update.get("update_id")
    if not mark_seen(name, uid):
        logger.info("dup.drop " + kv(name=name, uid=uid))
        return {"ok": True}

    msg = extract_msg(update)
    chat_id = (msg.get("chat") or {}).get("id")
    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    stale, age = is_stale(name, update)
    if stale and chat_id:
        logger.info("msg.stale " + kv(name=name, age=age))
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": f"⏱️ 요청이 오래되어 폐기되었습니다 (age={age}s). 다시 시도해 주세요.",
            "reply_to_message_id": message_id,
            "allow_sending_without_reply": True,
        }

    if not text or not chat_id:
        return {"ok": True}

    # whitelist (optional)
    if cfg["allowed_chats"]:
        if int(chat_id) not in cfg["allowed_chats"]:
            logger.info("chat.block " + kv(name=name, chat_id=chat_id))
            return {"ok": True}

    cmd = parse_command(text)
    if not cmd:
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "❓ 지원하지 않는 명령입니다.",
            "reply_to_message_id": message_id,
            "allow_sending_without_reply": True,
        }

    # 3) route to local WS bot (send only {text, tg_bot_name})
    target_bot_id = choose_bot_for(cmd, tg_bot_name=name)
    if not target_bot_id or target_bot_id not in bots_ws:
        logger.info("route.offline " + kv(name=name, cmd=cmd["type"]))
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "🤖 해당 로컬 봇이 오프라인입니다. 잠시 후 다시 시도해 주세요.",
            "reply_to_message_id": message_id,
            "allow_sending_without_reply": True,
        }

    corr_id = str(uuid.uuid4())
    waiter: asyncio.Future = asyncio.get_event_loop().create_future()
    bots_ws[target_bot_id].setdefault("waiters", {})[corr_id] = {"future": waiter}

    # dispatch to WS
    try:
        await bots_ws[target_bot_id]["ws"].send_text(json.dumps({
            "type": "task",
            "correlation_id": corr_id,
            "command": cmd["type"],
            "payload": {
                "text": cmd["raw_text"],
                "tg_bot_name": name,  # local_ws_bridge uses this to choose local backend (8000/8001/…)
            },
        }))
        logger.info("ws.dispatch " + kv(tg_bot=name, target=target_bot_id, cmd=cmd["type"], corr_id=corr_id))
    except Exception as e:
        bots_ws[target_bot_id]["waiters"].pop(corr_id, None)
        logger.warning("ws.dispatch.error " + kv(tg_bot=name, target=target_bot_id, err=str(e)))
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": f"📵 로컬 봇 전달 실패: {e}",
            "reply_to_message_id": message_id,
            "allow_sending_without_reply": True,
        }

    # await result and respond via webhook
    try:
        result: Dict[str, Any] = await asyncio.wait_for(waiter, timeout=BOT_RESPONSE_TIMEOUT_SEC)
        reply_text = result.get("text", "응답 형식 오류")
        logger.info("ws.result " + kv(corr_id=corr_id, size=len(reply_text or "")))
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": reply_text,
            "reply_to_message_id": message_id,
            "allow_sending_without_reply": True,
        }
    except asyncio.TimeoutError:
        logger.info("ws.result.timeout " + kv(corr_id=corr_id))
        # No delayed send (no token) → just return OK (no message)
        return {"ok": True}
    finally:
        bots_ws[target_bot_id]["waiters"].pop(corr_id, None)

# ─────────────────────────────────────────────────────────────────────
# Local bot WebSocket endpoint (authenticated)
@app.websocket("/ws/{bot_id}")
async def ws_bot(websocket: WebSocket, bot_id: str, token: str = Query(default="")):
    # Prefer Authorization header over query token
    auth = websocket.headers.get("Authorization", "")
    provided = auth[7:] if auth.startswith("Bearer ") else token

    expected = _expected_tokens_for(bot_id)  # <- set[str]
    if not expected or provided not in expected:
        await websocket.close(code=4401)
        logger.warning("ws.unauthorized " + kv(bot_id=bot_id))
        return

    await websocket.accept()

    try:
        hello_raw = await websocket.receive_text()
        hello = json.loads(hello_raw)
        caps = set(hello.get("caps", [])) if hello.get("type") == "hello" else set()
    except Exception:
        await websocket.close(code=1002)  # Protocol error
        logger.warning("ws.hello.bad " + kv(bot_id=bot_id))
        return

    bots_ws[bot_id] = {"ws": websocket, "caps": caps, "waiters": {}}
    logger.info("ws.online " + kv(bot_id=bot_id, caps=list(caps)))

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            if msg.get("type") == "result":
                corr_id = msg.get("correlation_id")
                info = bots_ws.get(bot_id, {}).get("waiters", {}).get(corr_id)
                if info:
                    fut: asyncio.Future = info["future"]
                    if not fut.done():
                        fut.set_result({"text": msg.get("text", "")})

            elif msg.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))

    except WebSocketDisconnect:
        pass
    finally:
        bots_ws.pop(bot_id, None)
        logger.info("ws.offline " + kv(bot_id=bot_id))

# ─────────────────────────────────────────────────────────────────────
# ↓↓↓ 여기부터 기존 Redis/데이터 API 합침 ↓↓↓

import json as _json
from pytz import timezone as _tz, utc as _utc  # noqa: F401 (utc 미사용 가능)
from datetime import datetime
import redis_client  # <- 같은 패키지에 redis_client.py 위치 가정
# storage 모듈 경로는 환경에 맞게 조정 (예: from telewebhook import storage)
from . import storage  # 같은 패키지 안에 storage.py 가 있다고 가정

@app.get("/")
def root():
    return {"message": "Hello, World!"}

@app.head("/")
def head_root():
    return {}

@app.get("/youtube")
def youtube_data():
    result = {}
    all_data = redis_client.hgetall("youtube_data")

    for country_bytes, raw_data_bytes in all_data.items():
        country = country_bytes.decode()
        try:
            raw_data = raw_data_bytes.decode()
            data = _json.loads(raw_data)
            result[country] = data

        except Exception as e:
            result[country] = {"error": f"{country} 처리 중 오류: {str(e)}"}
    return result

@app.get("/chartdata/{category}")
def get_chart_data(category: str):
    try:
        redis_key = "chart_data"  # HSET으로 저장된 hash key
        result = redis_client.hget(redis_key, category)

        if result:
            return _json.loads(result)  # JSON 파싱해서 dict 반환
        else:
            return {"error": f"'{category}'에 해당하는 데이터가 없습니다."}

    except Exception as e:
        return {"error": f"데이터 가져오기 실패: {str(e)}"}

@app.get("/market-holidays")
def get_market_holidays_api():
    result = {}
    try:
        all_data_raw = redis_client.hget("market_holidays", "all_holidays")
        timestamp_raw = redis_client.hget("market_holidays", "all_holidays_timestamp")

        if not all_data_raw or not timestamp_raw:
            result["error"] = "공휴일 데이터가 존재하지 않거나, 시간 정보가 없습니다."
            return result

        all_data = _json.loads(all_data_raw.decode())
        timestamp = timestamp_raw.decode()

        result["holidays"] = all_data
        result["timestamp"] = timestamp

        return result
    except Exception as e:
        result["error"] = f"공휴일 데이터를 가져오는 중 오류가 발생했습니다: {str(e)}"
        return result

@app.get("/daily-saved-data")
def get_daily_saved_data_api(page: int = 1, per_page: int = 5):
    try:
        all_dates = redis_client.hkeys("daily_saved_data")
        if not all_dates:
            return {"error": "저장된 daily_saved_data가 없습니다."}

        sorted_dates = sorted(all_dates, reverse=True)
        total = len(sorted_dates)

        start = (page - 1) * per_page
        end = start + per_page
        page_keys = sorted_dates[start:end]

        page_values = redis_client.hmget("daily_saved_data", page_keys)

        data = []
        for date, value in zip(page_keys, page_values):
            try:
                parsed = _json.loads(value)
            except Exception:
                parsed = {"error": "데이터 파싱 실패"}
            data.append({
                "date": date,
                "data": parsed
            })

        return {
            "total": total,
            "page": page,
            "perPage": per_page,
            "data": data
        }

    except Exception as e:
        return {"error": f"daily_saved_data 불러오는 중 오류: {str(e)}"}

@app.get("/test-save")
def test_save_endpoint():
    now = datetime.now(_tz('Asia/Seoul'))
    print("📈 chart data 저장 시작...")
    stored_result = storage.fetch_and_store_chart_data()
    print(stored_result)

    print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
    youtube_result = storage.fetch_and_store_youtube_data()
    print(youtube_result)
    try:
        timestamp_str = redis_client.hget("market_holidays", "all_holidays_timestamp")
        if timestamp_str:
            timestamp = datetime.strptime(timestamp_str.decode(), "%Y-%m-%dT%H:%M:%SZ")
            timestamp_kst = timestamp.replace(tzinfo=_tz('UTC')).astimezone(_tz('Asia/Seoul'))

            if timestamp_kst.date() == now.date():
                print("⏭️ 오늘 이미 휴일 데이터가 저장됨. 생략합니다.")
                return {"ok": True, "skipped": True}

        # 저장 안 되어 있거나 날짜가 오늘이 아니면 실행
        holiday_result = storage.fetch_and_store_holiday_data()
        print(holiday_result)

    except Exception as e:
        print(f"❌ Redis에서  timestamp 확인 중 오류 발생: {str(e)}")

    return {"ok": True}

@app.get("/test-code")
def test_code():
    return "test code실행"
