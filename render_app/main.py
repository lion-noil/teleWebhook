# render_app/main.py  (Render에 배포되는 앱)

import os, json
from fastapi import FastAPI, Request, HTTPException
import redis.asyncio as redis  # ← 비동기 클라이언트 권장 (redis>=5.0)

app = FastAPI()

# ===== 환경변수 =====
REDIS_URL = os.getenv("REDIS_URL")              # Upstash/Redis URL (rediss://...)
QUEUE_KEY = os.getenv("TG_QUEUE_KEY", "tg_updates")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # 있으면 /telegram/webhook/<secret> 사용

if not REDIS_URL:
    raise RuntimeError("환경변수 REDIS_URL이 필요합니다.")

# ===== Redis 클라이언트 =====
# decode_responses=True → 문자열로 다룸
r = redis.from_url(REDIS_URL, decode_responses=True)

# (선택) 헬스체크
@app.get("/healthz")
async def healthz():
    try:
        pong = await r.ping()
        return {"ok": pong is True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"redis ping fail: {e}")

# ===== Webhook 엔드포인트 =====
# SECRET 있으면 경로에 붙여서 보안 강화
webhook_path = "/telegram/webhook" + (f"/{WEBHOOK_SECRET}" if WEBHOOK_SECRET else "")

@app.post(webhook_path)
async def telegram_webhook(req: Request):
    # 텔레그램은 200 OK를 빨리 받는 걸 선호 → 최대한 빨리 ACK
    try:
        update = await req.json()
    except Exception:
        # JSON이 아니면 400
        raise HTTPException(status_code=400, detail="invalid json")

    # 필요하면 최소 필드만 추려서 넣어도 됨 (chat_id, text 등)
    # 여기선 원본 전체를 넣어 버퍼링
    try:
        await r.rpush(QUEUE_KEY, json.dumps(update))
    except Exception as e:
        # 큐 적재 실패해도 텔레그램 재전송 루프를 막고 싶으면 200을 반환
        # (재전송 유도하려면 5xx를 반환)
        # 여기서는 로깅만 가정
        print("rpush failed:", e)

    return {"ok": True}
