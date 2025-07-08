from api.__init__ import init
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class AllResourceList(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def all_resource_list(self):
        """
        모든 리소스 목록을 가져오는 메서드 <br>
        return: 모든 리소스 목록 (딕셔너리 형식) <br>
        """
        if self.customer_type == "pri":
            api_server = "https://resourcemanager.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://resourcemanager.apigw.gov-ntruss.com"
        uri = "/api/v1/resources"
        self.method = "POST"

        try:
            body = {}
            result = []
            uri_with_params = uri + "?responseFormatType=json"
            data = self.transform_uri(uri, api_server, body=body)

            # data 내에 error가 있는지 확인
            if "responseError" in data:
                error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                raise Exception(error_message)
            elif "error" in data:
                error_message = data["error"].get("message", "Unknown error occurred")
                raise Exception(error_message)
            
            if not len(data['items']) == 0:
                for resources in data['items']:
                    resources['customerName'] = self.customer_id
                    resources['Date'] = self.datetime
                    resources['region'] = resources['regionCode']
                    logging.info(f"{self.customer_id} - all_resource_list : " + resources['platformType'] + " " + resources['resourceType'] + " " + resources['region'])
                    result.append(resources)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/all_resource_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ['customerName', 'Date', 'platformType', 'resourceType', 'region']
                df = pd.DataFrame(result, columns=columns)
                df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
                df["platformType"] = df["platformType"].astype(str)
                df["resourceType"] = df["resourceType"].astype(str)
                df["region"] = df["region"].astype(str)
                table_name = "All_Resource_List"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching all resource list: {e}")
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
                        {"title": "Service Type:", "value": "all_resource_list"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result