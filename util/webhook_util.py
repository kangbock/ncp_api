import logging
import requests

class WebhookUtil:
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        """
        TeamsWebhook 초기화 메서드 <br>
        customer_id: 고객 ID <br>
        customer_type: 고객 유형 (pri 또는 gov) <br>
        access_key: 접근 키 <br>
        secret_key: 비밀 키 <br>
        """
        self.customer_id = customer_id
        self.customer_type = customer_type
        self.access_key = access_key
        self.secret_key = secret_key

    def send_webhook(self, body):
        """
        웹훅 알림을 보내는 메서드 <br>
        result: 결과 데이터 <br>
        Platform_type: 플랫폼 유형 <br>
        """
        # 웹훅 요청
        webhook_url = "WEBHOOK_URL"  # 실제 웹훅 URL로 변경해야 합니다.
        
        # 스키마에 맞는 attachments 생성
        attachments_content = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": body
        }
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": attachments_content
                }
            ]
        }
        try:
            requests.post(webhook_url, json=payload)
            logging.info(f"Webhook notification sent successfully")
        except Exception as webhook_exception:
            logging.error(f"Failed to send webhook notification: {webhook_exception}")