import os, json, time, uuid, asyncio
from collections import defaultdict, deque
from typing import Dict, Any, Optional, Set

from fastapi import FastAPI, Request, HTTPException, Header, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
import httpx

# ─────────────────────────────────────────────────────────────────────
# 환경변수
# BOTS_JSON 예시:
# [
#   {
#     "name": "bot1",
#     "token": "123456:ABC...",
#     "path_secret": "",
#     "header_secret": "",
#     "stale_seconds": 30,
#     "allowed_chats": [111111111]
#   }
# ]
BOTS_JSON = os.getenv("BOTS_JSON", "[]")
BOT_RESPONSE_TIMEOUT_SEC = int(os.getenv("BOT_RESPONSE_TIMEOUT_SEC", "5"))

BOT_CONNECT_TOKENS = json.loads(os.getenv("BOT_CONNECT_TOKENS_JSON", "{}"))

# ─────────────────────────────────────────────────────────────────────
# 설정 파싱
try:
    RAW_BOTS = json.loads(BOTS_JSON)
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
    }

app = FastAPI()

# ─────────────────────────────────────────────────────────────────────
# 중복/스테일 관리
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
# 텔레그램 유틸
async def tg_send_message(token: str, chat_id: int, text: str, reply_to_message_id: Optional[int] = None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
        if r.status_code != 200:
            print("[sendMessage error]", r.status_code, r.text)

# ─────────────────────────────────────────────────────────────────────
def parse_command(text: str) -> Optional[Dict[str, Any]]:
    up = (text or "").strip().upper()
    if up in ("/STATUS", "STATUS"):
        return {"type": "STATUS_QUERY"}
    return None

# ─────────────────────────────────────────────────────────────────────
# 로컬 봇(WebSocket) 레지스트리
# bot_id -> {"ws": WebSocket, "caps": set([...]), "waiters": {corr_id: {"future": fut, "token":..., "chat_id":..., "reply_to":...}}}
bots_ws: Dict[str, Dict[str, Any]] = {}

def choose_bot_for(cmd: Dict[str, Any]) -> Optional[str]:
    need = cmd["type"]
    for bot_id, info in bots_ws.items():
        if need in info.get("caps", set()):
            return bot_id
    return None

# ─────────────────────────────────────────────────────────────────────
@app.get("/healthz")
async def healthz():
    return {"ok": True, "telegram_bots": list(BOTS.keys()), "local_ws_bots": list(bots_ws.keys())}

@app.post("/manage/register-webhooks")
async def register_webhooks(base: str):
    """
    모든 텔레그램 봇에 대해 setWebhook:
    - url: {base}/telegram/webhook/{name}/{path_secret}
    - secret_token: header_secret (있다면)
    - drop_pending_updates=True
    """
    results = {}
    async with httpx.AsyncClient(timeout=15) as client:
        for name, cfg in BOTS.items():
            url = f"{base.rstrip('/')}/telegram/webhook/{name}/{cfg['path_secret']}"
            payload = {
                "url": url,
                "drop_pending_updates": True,
                "allowed_updates": ["message", "edited_message", "channel_post", "edited_channel_post"],
            }
            if cfg["header_secret"]:
                payload["secret_token"] = cfg["header_secret"]
            set_r = await client.post(f"https://api.telegram.org/bot{cfg['token']}/setWebhook", json=payload)
            info_r = await client.get(f"https://api.telegram.org/bot{cfg['token']}/getWebhookInfo")
            results[name] = {"set_status": set_r.status_code, "set_body": set_r.text, "info": info_r.json()}
    return results

# ─────────────────────────────────────────────────────────────────────
@app.post("/telegram/webhook/{name}/{secret}")
async def telegram_webhook(
    name: str,
    secret: str,
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # 1) 봇 검증
    if name not in BOTS:
        raise HTTPException(status_code=404)
    cfg = BOTS[name]
    if secret != cfg["path_secret"]:
        raise HTTPException(status_code=404)
    if cfg["header_secret"]:
        if not x_telegram_bot_api_secret_token or x_telegram_bot_api_secret_token != cfg["header_secret"]:
            raise HTTPException(status_code=403, detail="Bad secret header")

    # 2) 업데이트/중복/TTL
    update = await req.json()
    uid = update.get("update_id")
    if not mark_seen(name, uid):
        return {"ok": True}  # 중복 드롭

    msg = extract_msg(update)
    chat_id = (msg.get("chat") or {}).get("id")
    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    stale, age = is_stale(name, update)
    if stale and chat_id:
        await tg_send_message(cfg["token"], chat_id, f"⏱️ 요청이 오래되어 폐기되었습니다 (age={age}s). 다시 시도해 주세요.", reply_to_message_id=message_id)
        return {"ok": True}

    if not text:
        return {"ok": True}

    # 화이트리스트(선택)
    if cfg["allowed_chats"]:
        if not chat_id or int(chat_id) not in cfg["allowed_chats"]:
            return {"ok": True}

    # 3) 명령 파싱: status만
    cmd = parse_command(text)
    if not cmd:
        await tg_send_message(cfg["token"], chat_id, "❓ 지원하지 않는 명령입니다. 사용 가능: /status", reply_to_message_id=message_id)
        return {"ok": True}

    # 4) 로컬 봇 라우팅 (STATUS_QUERY만 처리)
    target_bot_id = choose_bot_for(cmd)
    if not target_bot_id or target_bot_id not in bots_ws:
        await tg_send_message(cfg["token"], chat_id, "🤖 처리 가능한 로컬 봇이 오프라인입니다. 잠시 후 다시 시도해 주세요.", reply_to_message_id=message_id)
        return {"ok": True}

    corr_id = str(uuid.uuid4())
    waiter: asyncio.Future = asyncio.get_event_loop().create_future()
    bots_ws[target_bot_id].setdefault("waiters", {})[corr_id] = {
        "future": waiter,
        "token": cfg["token"],
        "chat_id": chat_id,
        "reply_to": message_id,
    }

    # 작업 푸시
    try:
        await bots_ws[target_bot_id]["ws"].send_text(json.dumps({
            "type": "task",
            "correlation_id": corr_id,
            "command": "STATUS_QUERY",
            "payload": {}
        }))
    except Exception as e:
        bots_ws[target_bot_id]["waiters"].pop(corr_id, None)
        await tg_send_message(cfg["token"], chat_id, f"📵 로컬 봇 전달 실패: {e}", reply_to_message_id=message_id)
        return {"ok": True}

    # 응답 대기
    try:
        result: Dict[str, Any] = await asyncio.wait_for(waiter, timeout=BOT_RESPONSE_TIMEOUT_SEC)
        await tg_send_message(cfg["token"], chat_id, result.get("text", "응답 형식 오류"), reply_to_message_id=message_id)
    except asyncio.TimeoutError:
        await tg_send_message(cfg["token"], chat_id, "⏳ 로컬 봇 응답이 지연됩니다. 잠시 후 다시 시도해 주세요.", reply_to_message_id=message_id)
    finally:
        bots_ws[target_bot_id]["waiters"].pop(corr_id, None)

    return {"ok": True}

# ─────────────────────────────────────────────────────────────────────
# 로컬 봇 WebSocket 엔드포인트
@app.websocket("/ws/{bot_id}")
async def ws_bot(websocket: WebSocket, bot_id: str, token: str = Query(default="")):
    # 간단 토큰 인증
    expected = BOT_CONNECT_TOKENS.get(bot_id)
    if not expected or token != expected:
        await websocket.close(code=4401)  # Unauthorized
        return

    await websocket.accept()

    # 최초 hello 수신: {"type":"hello","caps":["STATUS_QUERY"]}
    try:
        hello_raw = await websocket.receive_text()
        hello = json.loads(hello_raw)
        caps = set(hello.get("caps", [])) if hello.get("type") == "hello" else set()
    except Exception:
        await websocket.close(code=1002)  # Protocol error
        return

    bots_ws[bot_id] = {"ws": websocket, "caps": caps, "waiters": {}}

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

            # 필요 시: 로컬 봇이 Render를 통해 직접 텔레그램 발송을 요청하도록 확장 가능
            # elif msg.get("type") == "send_telegram":
            #     await tg_send_message(info["token"], msg["chat_id"], msg["text"], reply_to_message_id=msg.get("reply_to"))
    except WebSocketDisconnect:
        pass
    finally:
        bots_ws.pop(bot_id, None)
