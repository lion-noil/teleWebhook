# 토큰 등록 Powershell
$TOKEN=""
$WEBHOOK="https://telewebhook.onrender.com/telegram/webhook/bot1/s1"
$SECRET=""

Invoke-RestMethod -Method Post `
  -Uri "https://api.telegram.org/bot$TOKEN/setWebhook" `
  -ContentType "application/json" `
  -Body (@{
    url = $WEBHOOK
    secret_token = $SECRET
    drop_pending_updates = $true
    allowed_updates = @("message","edited_message","channel_post","edited_channel_post")
  } | ConvertTo-Json)