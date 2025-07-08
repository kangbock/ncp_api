from util.webhook_util import WebhookUtil
import json
import hashlib
import hmac
import base64
import requests
import time
import datetime
import logging
import urllib.parse

# 로깅 설정 (전역적으로 사용)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M',
    handlers=[
        logging.StreamHandler(),  # 콘솔 출력
    ]
)

class init:
    def __init__(self, customer_id, customer_type, access_key, secret_key, timenow, method="GET"):
        """
        초기화 메서드 <br>
        customer_id: 고객 ID <br>
        customer_type: 고객 유형 (pri 또는 gov) <br>
        access_key: 접근 키 <br>
        secret_key: 비밀 키 <br>
        method: HTTP 메서드 (기본값: "GET") <br>
        datetime: 현재 시간 (YYYY-MM-DD HH:MM)
        """
        self.method = method
        self.customer_id = customer_id
        self.customer_type = customer_type
        self.access_key = access_key
        self.secret_key = secret_key
        self.datetime = timenow

    def date_time(self, customer_id=None):
        """
        현재 날짜와 시간을 반환하는 메서드 <br>
        return: 현재 날짜와 시간 (문자열 형식)
        """
        # 오늘 날짜
        today = datetime.datetime.now()

        # 이번 달의 연도와 월
        year = today.year
        month = today.month
        
        # 이번 주 월요일 계산
        this_week_monday = today - datetime.timedelta(days=today.weekday())
        # 저번 주 월요일 계산
        last_week_monday = this_week_monday - datetime.timedelta(weeks=1)

        # 만약 이번 달이 1월이라면, 저번 달은 전년도 12월이 됨
        if month == 1:
            prev_year = year - 1
            prev_month = 12
        else:
            prev_year = year
            prev_month = month - 1

        # 저번 달 1일 00:00:00 계산
        first_day_prev_month = datetime.datetime(prev_year, prev_month, 1)
        # 저번 달 31일 23:59:59 계산
        if prev_month == 12:
            last_day_prev_month = datetime.datetime(prev_year, prev_month, 31, 23, 59, 59)
        else:
            last_day_prev_month = datetime.datetime(prev_year, prev_month + 1, 1) - datetime.timedelta(seconds=1)

        # 저번 달 20일부터 이번 달 20일까지의 데이터가 필요한 고객
        if customer_id == "CUSTOMER_ID_20TH":
            first_day_prev_month = datetime.datetime(prev_year, prev_month, 20)
            last_day_prev_month = datetime.datetime(year, month, 20, 23, 59, 59)

        # 저번 주 월요일 10시부터 이번 주 월요일 10시까지의 데이터가 필요한 고객 (주간보고)
        if customer_id == "CUSTOMER_ID_WEEKLY":
            first_day_prev_month = last_week_monday.replace(hour=10, minute=0, second=0, microsecond=0)
            last_day_prev_month = this_week_monday.replace(hour=10, minute=0, second=0, microsecond=0)

        # 저번 달 1일 00:00:00을 UTC 타임스탬프로 변환
        start_time = int(first_day_prev_month.timestamp() * 1000)
        # 저번 달 31일 23:59:59을 UTC 타임스탬프로 변환
        end_time = int(last_day_prev_month.timestamp() * 1000)

        # 저번 달 1일 00:00:00과 저번 달 31일 23:59:59을 문자열로 변환
        str_start_time = first_day_prev_month.strftime("%Y-%m-%d %H:%M:%S")
        str_end_time = last_day_prev_month.strftime("%Y-%m-%d %H:%M:%S")

        return start_time, end_time, str_start_time, str_end_time
    
    def nas_date_time(self,):
        """
        어제 0시부터 오늘 0시까지의 시간을 URL 인코딩된 ISO 8601 형식으로 반환
        """
        # 오늘 날짜
        today = datetime.datetime.now()
        # 어제 날짜 계산
        yesterday = today - datetime.timedelta(days=1)

        # 어제 0시 (ISO 8601 형식)
        str_yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S+0000")
        # 오늘 0시 (ISO 8601 형식)
        str_today = today.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S+0000")

        # URL 인코딩 적용
        startTime = urllib.parse.quote(str_yesterday)
        endTime = urllib.parse.quote(str_today)

        return startTime, endTime

        

    def transform_uri(self, uri, api_server, body=None):
        """
        URI를 변환하고 API 서버에 요청을 보냄 <br>
        uri: 요청 URI <br>
        api_server: API 서버 주소 <br>
        return: 요청 결과 (JSON 형식) <br>
        """
        # unix timestamp setting
        timestamp = int(time.time() * 1000)
        timestamp = str(timestamp)

        message = self.method + " " + uri + "\n" + timestamp + "\n" + self.access_key
        message = bytes(message, 'UTF-8')
        secret_key = bytes(self.secret_key, 'UTF-8')
        signingKey = base64.b64encode(hmac.new(secret_key, message, digestmod=hashlib.sha256).digest())

        http_header = {
            'Content-type': 'application/json',
            'x-ncp-apigw-timestamp': timestamp,
            'x-ncp-iam-access-key': self.access_key,
            'x-ncp-apigw-signature-v2': signingKey,
        }

        try:
            if self.method == "GET":
                response = requests.get(api_server + uri, headers=http_header)
                result = json.loads(response.text)
            elif self.method == "POST":
                response = requests.post(api_server + uri, headers=http_header, json=body)
                result = json.loads(response.text)

            return result
        
        except Exception as e:
            logging.error(f"{self.customer_id} - Error in transform_uri: {e}")
            # 웹훅 body 생성
            webhook_body = [
                {
                    "type": "TextBlock",
                    "text": "NCP Monthly Report Exception Error",
                    "weight": "Bolder",
                    "size": "Medium"
                },
                {
                    "type": "FactSet",
                    "facts": [
                        {"title": "Customer ID:", "value": self.customer_id},
                        {"title": "Customer Type:", "value": self.customer_type},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri},
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, webhook_body)