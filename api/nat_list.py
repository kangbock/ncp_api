from api.__init__ import init
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class NAT(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def vpc_nat_list(self):
        """
        VPC NAT 목록을 조회하는 메서드 <br>
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/vpc/v2/getNatGatewayInstanceList"
        uri_with_params = uri + "?responseFormatType=json"

        try:
            result = []
            Platform_type = 'VPC'
            data = init.transform_uri(self, uri_with_params, api_server)
            
            # data 내에 error가 있는지 확인
            if "responseError" in data:
                error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                raise Exception(error_message)
            elif "error" in data:
                error_message = data["error"].get("message", "Unknown error occurred")
                raise Exception(error_message)
            
            if not len(data['getNatGatewayInstanceListResponse']['natGatewayInstanceList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} NAT Gateway Total Number : "  + str(len(data['getNatGatewayInstanceListResponse']['natGatewayInstanceList'])))

                for natgw in data['getNatGatewayInstanceListResponse']['natGatewayInstanceList']:
                    natgw['customer_id'] = self.customer_id
                    natgw["datetime"] = self.datetime
                    logging.info(f"{self.customer_id} - {Platform_type} NAT Gateway Name : " + natgw['natGatewayName'])
                    result.append(natgw)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_nat_gateway_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "natGatewayName", "natGatewayInstanceStatusName", "publicIp", "privateIp", "zoneCode"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                table_name = f"{self.customer_type}_{Platform_type}_nat_gateway_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching nat gateway list: {e}")
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
                        {"title": "Platform Type:", "value": Platform_type},
                        {"title": "Service Type:", "value": "NAT Gateway"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result