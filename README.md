# 값 넣기
$Name         = ''     # 주: 공백 제거 권장
$Token        = ''         # @BotFather 토큰
$PathSecret   = 's1'
$HeaderSecret = 'h1'
$Base         = 'https://telewebhook.onrender.com'

# 토큰 검증(선택)
Invoke-RestMethod -Uri "https://api.telegram.org/bot$Token/getMe" -Method Get

# Webhook 등록
$WebhookUrl = "$Base/telegram/webhook/$Name/$PathSecret"
$body = @{
  url                  = $WebhookUrl
  secret_token         = $HeaderSecret
  drop_pending_updates = $true
  allowed_updates      = @("message")
} | ConvertTo-Json -Compress

Invoke-RestMethod -Uri "https://api.telegram.org/bot$Token/setWebhook" `
  -Method Post -ContentType 'application/json' -Body $body

# 확인
Invoke-RestMethod -Uri "https://api.telegram.org/bot$Token/getWebhookInfo" -Method Get


# 웹훅 삭제 (대기 중 업데이트 버리기 옵션 포함)
Invoke-RestMethod -Method Post -Uri "https://api.telegram.org/bot$Token/deleteWebhook" `
  -ContentType 'application/json' `
  -Body (@{ drop_pending_updates = $true } | ConvertTo-Json -Compress)

# 3) 다시 확인 (url 이 비어있거나, pending_update_count가 0이면 OK)
Invoke-RestMethod -Method Get -Uri "https://api.telegram.org/bot$Token/getWebhookInfo"