import os
import json
import time
import uuid
import asyncio
import logging
from collections import defaultdict, deque
from typing import Dict, Any, Optional, Set

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, HTTPException, Header, WebSocket, WebSocketDisconnect, Query
import httpx

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
# ENV
BOTS_JSON = os.getenv("BOTS_JSON", "[]")
BOT_RESPONSE_TIMEOUT_SEC = int(os.getenv("BOT_RESPONSE_TIMEOUT_SEC", "5"))
BOT_CONNECT_TOKENS_JSON = os.getenv("BOT_CONNECT_TOKENS_JSON", "{}")
BOT_CONNECT_TOKENS_FILE = os.getenv("BOT_CONNECT_TOKENS_FILE")

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
# Parse BOTS
try:
    RAW_BOTS = json.loads(_strip_quotes(BOTS_JSON))
except Exception as e:
    raise RuntimeError(f"BOTS_JSON 파싱 실패: {e}")

BOTS: Dict[str, Dict[str, Any]] = {}
for b in RAW_BOTS:
    if not b.get("name") or not b.get("token") or not b.get("path_secret"):
        raise RuntimeError("각 bot 엔트리는 name/token/path_secret 이 필요합니다.")
    BOTS[b["name"]] = {
        "token": b["token"],
        "path_secret": b["path_secret"],
        "header_secret": b.get("header_secret") or "",
        "stale_seconds": int(b.get("stale_seconds") or 60),
        "allowed_chats": b.get("allowed_chats") or [],
        # Optional mapping: Telegram bot name -> local WS bot id
        "ws_bot_id": b.get("ws_bot_id"),
    }

# ─────────────────────────────────────────────────────────────────────
# Load WS connect tokens (for /ws/{bot_id})
TOKENS: Dict[str, Any] = {}
try:
    if BOT_CONNECT_TOKENS_FILE and os.path.exists(BOT_CONNECT_TOKENS_FILE):
        with open(BOT_CONNECT_TOKENS_FILE, encoding="utf-8") as f:
            TOKENS = json.load(f)
    else:
        TOKENS = _load_json_env("BOT_CONNECT_TOKENS_JSON", "{}")
except Exception as e:
    raise RuntimeError(f"BOT_CONNECT_TOKENS 로드 실패: {e}")


def _expected_tokens_for(bot_id: str) -> list:
    v = TOKENS.get(bot_id)
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


# ─────────────────────────────────────────────────────────────────────
app = FastAPI()

# Dedup / stale mgmt
_seen_ids: Dict[str, Set[int]] = defaultdict(set)  # botname -> seen update_ids
_seen_qs: Dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))


def mark_seen(botname: str, uid: Optional[int]) -> bool:
    if uid is None:
        return True
    if uid in _seen_ids[botname]:
        return False
    _seen_ids[botname].add(uid)
    _seen_qs[botname].append(uid)
    return True


def extract_msg(update: Dict[str, Any]) -> Dict[str, Any]:
    return (
        update.get("message")
        or update.get("channel_post")
        or update.get("edited_message")
        or update.get("edited_channel_post")
        or {}
    )


def is_stale(botname: str, update: Dict[str, Any]) -> tuple[bool, int]:
    msg = extract_msg(update)
    ts = msg.get("date")
    if not ts:
        return (False, 0)
    age = int(time.time() - int(ts))
    return (age > BOTS[botname]["stale_seconds"], age)


# ─────────────────────────────────────────────────────────────────────
# Telegram util
async def tg_send_message(token: str, chat_id: int, text: str, reply_to_message_id: Optional[int] = None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True
    timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
        if r.status_code != 200:
            logger.warning("sendMessage.error " + kv(status=r.status_code, body=r.text))


# ─────────────────────────────────────────────────────────────────────
# Commands

def parse_command(text: str) -> Optional[Dict[str, Any]]:
    up = (text or "").strip().lower()
    if up in ("/status", "status"):
        return {"type": "STATUS_QUERY"}
    return None


# ─────────────────────────────────────────────────────────────────────
# Local WS registry
# bot_id -> {"ws": WebSocket, "caps": set([...]), "waiters": {corr_id: {"future": fut, "token":..., "chat_id":..., "reply_to":...}}}
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


@app.post("/manage/register-webhooks")
async def register_webhooks(base: str):
    """
    For each Telegram bot, call setWebhook:
      url: {base}/telegram/webhook/{name}/{path_secret}
      secret_token: header_secret (if set)
      drop_pending_updates=True
    """
    results = {}
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for name, cfg in BOTS.items():
            url = f"{base.rstrip('/')}/telegram/webhook/{name}/{cfg['path_secret']}"
            payload = {
                "url": url,
                "drop_pending_updates": True,
                "allowed_updates": [
                    "message", "edited_message", "channel_post", "edited_channel_post"
                ],
            }
            if cfg["header_secret"]:
                payload["secret_token"] = cfg["header_secret"]
            set_r = await client.post(f"https://api.telegram.org/bot{cfg['token']}/setWebhook", json=payload)
            info_r = await client.get(f"https://api.telegram.org/bot{cfg['token']}/getWebhookInfo")
            results[name] = {"set_status": set_r.status_code, "set_body": set_r.text, "info": info_r.json()}
            logger.info("register_webhook " + kv(name=name, url=url, status=set_r.status_code))
    return results


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
            raise HTTPException(status_code=403, detail="Bad secret header")

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
        await tg_send_message(cfg["token"], chat_id, f"⏱️ 요청이 오래되어 폐기되었습니다 (age={age}s). 다시 시도해 주세요.", reply_to_message_id=message_id)
        logger.info("msg.stale " + kv(name=name, age=age))
        return {"ok": True}

    if not text:
        return {"ok": True}

    # whitelist (optional)
    if cfg["allowed_chats"]:
        if not chat_id or int(chat_id) not in cfg["allowed_chats"]:
            logger.info("chat.block " + kv(name=name, chat_id=chat_id))
            return {"ok": True}

    cmd = parse_command(text)
    if not cmd:
        await tg_send_message(cfg["token"], chat_id, "❓ 지원하지 않는 명령입니다. 사용 가능: /status", reply_to_message_id=message_id)
        return {"ok": True}

    # 3) route to local WS bot
    target_bot_id = choose_bot_for(cmd, tg_bot_name=name)
    if not target_bot_id or target_bot_id not in bots_ws:
        await tg_send_message(cfg["token"], chat_id, "🤖 처리 가능한 로컬 봇이 오프라인입니다. 잠시 후 다시 시도해 주세요.", reply_to_message_id=message_id)
        logger.info("route.offline " + kv(name=name, cmd=cmd["type"]))
        return {"ok": True}

    corr_id = str(uuid.uuid4())
    waiter: asyncio.Future = asyncio.get_event_loop().create_future()
    bots_ws[target_bot_id].setdefault("waiters", {})[corr_id] = {
        "future": waiter,
        "token": cfg["token"],
        "chat_id": chat_id,
        "reply_to": message_id,
    }

    # dispatch
    try:
        await bots_ws[target_bot_id]["ws"].send_text(json.dumps({
            "type": "task",
            "correlation_id": corr_id,
            "command": cmd["type"],
            "payload": {},
        }))
        logger.info("ws.dispatch " + kv(tg_bot=name, target=target_bot_id, cmd=cmd["type"], corr_id=corr_id))
    except Exception as e:
        bots_ws[target_bot_id]["waiters"].pop(corr_id, None)
        await tg_send_message(cfg["token"], chat_id, f"📵 로컬 봇 전달 실패: {e}", reply_to_message_id=message_id)
        logger.warning("ws.dispatch.error " + kv(tg_bot=name, target=target_bot_id, err=str(e)))
        return {"ok": True}

    # await result
    try:
        result: Dict[str, Any] = await asyncio.wait_for(waiter, timeout=BOT_RESPONSE_TIMEOUT_SEC)
        await tg_send_message(cfg["token"], chat_id, result.get("text", "응답 형식 오류"), reply_to_message_id=message_id)
        logger.info("ws.result " + kv(corr_id=corr_id, size=len(result.get("text", "") or "")))
    except asyncio.TimeoutError:
        await tg_send_message(cfg["token"], chat_id, "⏳ 로컬 봇 응답이 지연됩니다. 잠시 후 다시 시도해 주세요.", reply_to_message_id=message_id)
        logger.info("ws.result.timeout " + kv(corr_id=corr_id))
    finally:
        bots_ws[target_bot_id]["waiters"].pop(corr_id, None)

    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────
# Local bot WebSocket endpoint
@app.websocket("/ws/{bot_id}")
async def ws_bot(websocket: WebSocket, bot_id: str, token: str = Query(default="")):
    # Prefer Authorization header over query token
    auth = websocket.headers.get("Authorization", "")
    provided = auth[7:] if auth.startswith("Bearer ") else token

    expected_list = _expected_tokens_for(bot_id)
    if not expected_list or provided not in expected_list:
        await websocket.close(code=4401)  # Unauthorized
        logger.warning("ws.unauthorized " + kv(bot_id=bot_id))
        return

    await websocket.accept()

    # Expect initial hello {"type":"hello","caps":[...]} from client
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

            # (optional) support for direct telegram send by local bot in future
            # elif msg.get("type") == "send_telegram":
            #     await tg_send_message(info["token"], msg["chat_id"], msg["text"], reply_to_message_id=msg.get("reply_to"))

    except WebSocketDisconnect:
        pass
    finally:
        bots_ws.pop(bot_id, None)
        logger.info("ws.offline " + kv(bot_id=bot_id))
