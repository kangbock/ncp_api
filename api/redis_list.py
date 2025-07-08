from api.__init__ import init
from api.vpc_list import VPC
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class Redis(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def redis_list(self):
        """
        Redis 서비스 리스트 조회
        return: Redis 서비스 리스트 (딕셔너리 형식)
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        result = []
        raw_data = []
        uri = "/vredis/v2/getCloudRedisInstanceList"

        try:
            uri_with_params = uri + "?responseFormatType=json"
            data = self.transform_uri(uri_with_params, api_server)

            # data 내에 error가 있는지 확인
            if "responseError" in data:
                error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                raise Exception(error_message)
            elif "error" in data:
                error_message = data["error"].get("message", "Unknown error occurred")
                raise Exception(error_message)
            
            if not len(data["getCloudRedisInstanceListResponse"]["cloudRedisInstanceList"]) == 0:
                logging.info(f"{self.customer_id} - VPC Redis Service Total Number : "  + str(len(data["getCloudRedisInstanceListResponse"]["cloudRedisInstanceList"])))

                for redis in data["getCloudRedisInstanceListResponse"]["cloudRedisInstanceList"]:
                    for redisserver in redis["cloudRedisServerInstanceList"]:
                        redis_data = {
                            "customer_id": self.customer_id,
                            "datetime": self.datetime,
                            "serviceName": redis["cloudRedisServiceName"],
                            "serverName": redisserver["cloudRedisServerName"],
                            "serverRole": redisserver["cloudRedisServerRole"]["codeName"],
                            "status": redisserver["cloudRedisServerInstanceStatusName"],
                            "vpcName": VPC.vpc_list(self, redisserver["vpcNo"], redisserver["regionCode"]),
                            "zoneCode": redisserver.get("zoneCode", "Unknown"),
                            "cpuCount": redisserver.get("cpuCount", 0),
                            "memorySize": round(redisserver["memorySize"] / (1024**3), 1),
                        }
                        result.append(redis_data)                    
                        logging.info(f"{self.customer_id} - VPC Redis : " + redisserver["cloudRedisServerName"])
                    raw_data.append(redis)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_VPC_redis_list.json"
                JSONUtil.save_to_json(result, json_file_path)
                
                # raw data 확인
                # JSONUtil.save_to_json(raw_data, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "serviceName", "serverName", "serverRole", "status", "vpcName", "zoneCode", "cpuCount", "memorySize"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df["cpuCount"] = df["cpuCount"].astype(int)
                df["memorySize"] = df["memorySize"].astype(float)
                table_name = f"{self.customer_type}_VPC_redis_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching redis list: {e}")
            # 웹훅 body 생성
            body = [
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
                        {"title": "Platform Type:", "value": "VPC"},
                        {"title": "Service Type:", "value": "Redis"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result