# telewebhook/main.py
# Tokenless telewebhook:
# - Receives Telegram webhook updates
# - Forwards {text, tg_bot_name} over local WS to local_ws_bridge
# - Returns Telegram Bot API call (sendMessage) in the webhook HTTP response (no bot token needed)
# - WS authentication uses BOT_CONNECT_TOKENS_JSON / BOT_CONNECT_TOKENS_FILE

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

# ─────────────────────────────────────────────────────────────────────
# Logging (logfmt style)
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

# WS auth for /ws/{bot_id}

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

def _load_json_env(name: str, default: str) -> Any:
    raw = os.getenv(name)
    if not raw:
        return json.loads(default)
    return json.loads(_strip_quotes(raw))

# ─────────────────────────────────────────────────────────────────────
# Parse BOTS (no token needed)
# Each bot entry requires: name, path_secret
# Optional: header_secret (X-Telegram-Bot-Api-Secret-Token), stale_seconds, allowed_chats, ws_bot_id
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
        # Optional mapping: Telegram bot name -> local WS bot id
        "ws_bot_id": b.get("ws_bot_id"),
    }

# ─────────────────────────────────────────────────────────────────────
# Load WS connect tokens (for /ws/{bot_id})
# Build WS tokens from BOTS_JSON
def _coerce_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

# 토큰 컨테이너는 "집합"이 가장 안전(중복 제거/멤버십 빠름)
TOKENS: Dict[str, set[str]] = {}


for name, cfg in BOTS.items():
    ws_id = (cfg.get("ws_bot_id") or name).strip()
    ws_tokens = {str(t).strip() for t in _coerce_list(cfg.get("ws_token")) if str(t).strip()}
    if ws_id and ws_tokens:
        TOKENS.setdefault(ws_id, set()).update(ws_tokens)

def _expected_tokens_for(bot_id: str) -> list:
    v = TOKENS.get(bot_id)
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


# Build WS tokens from BOTS_JSON ... (TOKENS 구성 코드 바로 아래)

def _preview(tok: Optional[str]):
    if not tok:
        return None
    s = str(tok)
    return (s[:2] + "..." + s[-2:]) if len(s) > 4 else "***"

# (선택) 부팅 시 현재 등록된 bot_id들과 토큰 개수 로그
logger.info("ws.auth.config " + kv(
    ids=list(TOKENS.keys()),
    sizes={k: len(v) for k, v in TOKENS.items()},
))
# ─────────────────────────────────────────────────────────────────────
app = FastAPI()
logger.info("ws.auth.config " + kv(
    ids=list(TOKENS.keys()),
    sizes={k: len(v) for k, v in TOKENS.items()},
))
# Dedup / stale mgmt
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

def extract_msg(update: Dict[str, Any]) -> Dict[str, Any]:
    return update.get("message") or {}


def is_stale(botname: str, update: Dict[str, Any]) -> Tuple[bool, int]:
    msg = extract_msg(update)
    ts = msg.get("date")
    if not ts:
        return (False, 0)
    age = int(time.time() - int(ts))
    return (age > BOTS[botname]["stale_seconds"], age)

# ─────────────────────────────────────────────────────────────────────
# Commands: pass-through (text only)
def parse_command(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    return {"type": "TEXT_COMMAND", "raw_text": text}

# ─────────────────────────────────────────────────────────────────────
# Local WS registry
# bot_id -> {"ws": WebSocket, "caps": set([...]), "waiters": {corr_id: {"future": fut}}}
bots_ws: Dict[str, Dict[str, Any]] = {}

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
@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "telegram_bots": list(BOTS.keys()),
        "local_ws_bots": list(bots_ws.keys()),
        "seen": {k: len(v) for k, v in _seen_ids.items()},
    }

# ─────────────────────────────────────────────────────────────────────
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
# Local bot WebSocket endpoint (authenticated by BOT_CONNECT_TOKENS)
@app.websocket("/ws/{bot_id}")
async def ws_bot(websocket: WebSocket, bot_id: str, token: str = Query(default="")):
    # Prefer Authorization header over query token
    auth = websocket.headers.get("Authorization", "")
    provided = auth[7:] if auth.startswith("Bearer ") else token

    expected_list = _expected_tokens_for(bot_id)
    logger.info("ws.auth.check " + kv(
        bot_id=bot_id,
        provided_in=("header" if auth.startswith("Bearer ") else ("query" if token else "none")),
        provided_preview=_preview(provided),
        expect_cnt=len(expected_list),
    ))


    if not expected_list or provided not in expected_list:
        await websocket.close(code=4401)  # Unauthorized
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

            # (optional) future extension:
            # elif msg.get("type") == "send_telegram":
            #     # Not supported in tokenless mode (no delayed messages)

    except WebSocketDisconnect:
        pass
    finally:
        bots_ws.pop(bot_id, None)
        logger.info("ws.offline " + kv(bot_id=bot_id))