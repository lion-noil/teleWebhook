# app/main.py
import os, json, time, asyncio
from collections import deque, defaultdict
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request, HTTPException, Header, Query
import httpx

# ── 환경설정 ───────────────────────────────────────────────────────────
# BOTS_JSON 예시:
# [
#   {
#     "name": "bot1",
#     "token": "123456:ABC...",
#     "path_secret": "s1",                 # /telegram/webhook/{name}/{secret} 의 secret
#     "header_secret": "h1",               # setWebhook(secret_token)과 매칭(선택)
#     "stale_seconds": 60,                 # 오래된 업데이트 드롭 기준(초, 기본 60)
#     "allowed_chats": [111111, 222222],   # 허용 채팅ID 화이트리스트(선택)
#     "status_url": "https://your-backend/status?plain=true"  # /BTCUSDT 처리용(선택)
#   }
# ]
BOTS_JSON = os.getenv("BOTS_JSON", "[]")

try:
    RAW_BOTS: List[Dict[str, Any]] = json.loads(BOTS_JSON)
except Exception as e:
    raise RuntimeError(f"BOTS_JSON 파싱 실패: {e}")

# name -> config
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
        "status_url": b.get("status_url") or "",
    }

app = FastAPI()

# 중복/스테일 관리 (프로세스 메모리 내)
_seen_ids: Dict[str, set] = defaultdict(set)         # botname -> set(update_id)
_seen_qs: Dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))

def mark_seen(botname: str, uid: Optional[int]) -> bool:
    if uid is None:  # 안전 차원: uid 없으면 중복 판정 안 함
        return True
    s = _seen_ids[botname]
    if uid in s:
        return False
    s.add(uid)
    _seen_qs[botname].append(uid)
    return True

def is_stale(botname: str, update: Dict[str, Any]) -> bool:
    cfg = BOTS[botname]
    stale_seconds = cfg["stale_seconds"]
    msg = (
        update.get("message")
        or update.get("channel_post")
        or update.get("edited_message")
        or update.get("edited_channel_post")
        or {}
    )
    ts = msg.get("date")
    if not ts:
        return False
    age = time.time() - int(ts)
    return age > stale_seconds

async def send_text(token: str, chat_id: int, text: str):
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )
        # 필요하면 실패 로깅
        if r.status_code != 200:
            print("[sendMessage error]", r.status_code, r.text)

# ── 라우트 ────────────────────────────────────────────────────────────
@app.get("/healthz")
async def healthz():
    return {"ok": True, "bots": list(BOTS.keys())}

@app.get("/manage/list-bots")
async def list_bots():
    # 민감 정보(token 등) 제거
    safe = []
    for name, cfg in BOTS.items():
        safe.append({
            "name": name,
            "path_secret": "***",
            "header_secret": "***" if cfg["header_secret"] else "",
            "stale_seconds": cfg["stale_seconds"],
            "allowed_chats": cfg["allowed_chats"],
            "status_url": cfg["status_url"],
        })
    return {"count": len(safe), "bots": safe}

@app.post("/manage/register-webhooks")
async def register_webhooks(
    base: str = Query(..., description="공개 베이스 URL, 예: https://your-service.onrender.com")
):
    """
    모든 봇에 대해 setWebhook 수행.
    - base/telegram/webhook/{name}/{path_secret} 로 등록
    - header_secret이 있으면 secret_token으로 세팅
    - drop_pending_updates=True
    """
    results = {}
    async with httpx.AsyncClient(timeout=10) as client:
        for name, cfg in BOTS.items():
            url = f"{base.rstrip('/')}/telegram/webhook/{name}/{cfg['path_secret']}"
            payload = {
                "url": url,
                "drop_pending_updates": True,
                "allowed_updates": ["message", "channel_post"],
            }
            if cfg["header_secret"]:
                payload["secret_token"] = cfg["header_secret"]

            r = await client.post(
                f"https://api.telegram.org/bot{cfg['token']}/setWebhook",
                json=payload
            )
            info = (await client.get(
                f"https://api.telegram.org/bot{cfg['token']}/getWebhookInfo"
            )).json()
            results[name] = {
                "set_status": r.status_code,
                "set_body": r.text,
                "info": info,
            }
    return results

@app.post("/telegram/webhook/{name}/{secret}")
async def telegram_webhook(
    name: str,
    secret: str,
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # 1) 봇 존재/시크릿 검증
    if name not in BOTS:
        raise HTTPException(status_code=404)
    cfg = BOTS[name]
    if secret != cfg["path_secret"]:
        raise HTTPException(status_code=404)

    # 2) 헤더 시크릿(선택) 검증
    if cfg["header_secret"]:
        if not x_telegram_bot_api_secret_token or x_telegram_bot_api_secret_token != cfg["header_secret"]:
            raise HTTPException(status_code=403, detail="Bad secret header")

    # 3) 업데이트 파싱
    update = await req.json()
    uid = update.get("update_id")

    # 4) 중복/오래된 업데이트 드롭
    if not mark_seen(name, uid):
        print(f"[drop] dup update_id={uid} ({name})")
        return {"ok": True}
    if is_stale(name, update):
        print(f"[drop] stale update_id={uid} ({name})")
        return {"ok": True}

    msg = update.get("message") or update.get("channel_post") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    text = (msg.get("text") or "").strip()

    # 5) 허용 채팅 화이트리스트(선택)
    if cfg["allowed_chats"]:
        if not chat_id or int(chat_id) not in cfg["allowed_chats"]:
            # 무시(로그만)
            print(f"[drop] not allowed chat {chat_id} for bot {name}")
            return {"ok": True}

    # 6) 명령 처리
    reply = None
    if text:
        up = text.upper()
        if up in ("/PING", "PING"):
            reply = "pong"
        elif up in ("/HELP", "HELP"):
            reply = "사용 가능: /PING, /HELP, /ECHO <msg>, /BTCUSDT(옵션)"
        elif up.startswith("/ECHO"):
            reply = text.partition(" ")[2] or "(empty)"
        elif up in ("/BTCUSDT", "BTCUSDT"):
            # 옵션: status_url이 있으면 그 값을 그대로 반환
            if cfg["status_url"]:
                try:
                    async with httpx.AsyncClient(timeout=8) as client:
                        r = await client.get(cfg["status_url"])
                        reply = r.text if r.status_code == 200 else "상태 조회 실패"
                except Exception as e:
                    reply = f"상태 조회 오류: {e}"
            else:
                reply = "상태 URL이 설정되지 않았습니다."
        else:
            reply = "지원하지 않는 명령입니다. /HELP"

    # 7) 회신
    if chat_id and reply:
        await send_text(cfg["token"], chat_id, reply)

    return {"ok": True}
