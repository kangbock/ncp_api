from api.__init__ import init
from api.server_list import ServerList
from api.product import Product
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class ServerImageList(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def classic_serverimage_list(self, serverImageNo=None):
        """
        서버 이미지 목록을 가져오는 메서드 <br>
        return: 서버 이미지 목록 (딕셔너리 형식)
        KVM 서버의 내 서버 이미지는 지원되지 않습니다.
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/server/v2/getMemberServerImageList"


        try:
            result = []
            Platform_type = 'Classic'
            uri_with_params = uri + "?responseFormatType=json"
            data = self.transform_uri(uri_with_params, api_server)

            # data 내에 error가 있는지 확인
            if "responseError" in data:
                error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                raise Exception(error_message)
            elif "error" in data:
                error_message = data["error"].get("message", "Unknown error occurred")
                raise Exception(error_message)
            
            if not len(data['getMemberServerImageListResponse']['memberServerImageList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} Server Image Total Number : " + str(len(data['getMemberServerImageListResponse']['memberServerImageList'])))
                for serverimage in data['getMemberServerImageListResponse']['memberServerImageList']:
                    serverimage['customer_id'] = self.customer_id
                    serverimage['datetime'] = self.datetime
                    serverimage['serverImageName'] = serverimage['memberServerImageName']
                    serverimage['originServer'] = serverimage['originalServerName'] or ""
                    serverimage['originImage'] = serverimage['originalServerImageName']
                    serverimage['imageStatus'] = serverimage['memberServerImageStatusName']
                    serverimage['blockStorageSize'] = serverimage['memberServerImageBlockStorageTotalSize'] // (1024**3)
                    logging.info(f"{self.customer_id} - {Platform_type} Server Image Name : " + serverimage['memberServerImageName'])
                    result.append(serverimage)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_serverimage_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "serverImageName", "originServer", "originImage", "imageStatus", "blockStorageSize"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                df["blockStorageSize"] = df["blockStorageSize"].astype(int)
                df["originImage"] = df["originImage"].astype(str)
                table_name = f"{self.customer_type}_{Platform_type}_serverimage_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching server image list: {e}")
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
                        {"title": "Service Type:", "value": "Server Image"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    def vpc_serverimage_list(self, serverImageNo=None):
        """
        서버 이미지 목록을 가져오는 메서드 <br>
        return: 서버 이미지 목록 (딕셔너리 형식)
        KVM 서버의 내 서버 이미지는 지원되지 않습니다.
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/vserver/v2/getMemberServerImageInstanceList"

        try:
            result = []
            Platform_type = 'VPC'
            uri_with_params = uri + "?responseFormatType=json"
            data = self.transform_uri(uri_with_params, api_server)

            # data 내에 error가 있는지 확인
            if "responseError" in data:
                error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                raise Exception(error_message)
            elif "error" in data:
                error_message = data["error"].get("message", "Unknown error occurred")
                raise Exception(error_message)
            
            if not len(data['getMemberServerImageInstanceListResponse']['memberServerImageInstanceList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} Server Image Total Number : " + str(len(data['getMemberServerImageInstanceListResponse']['memberServerImageInstanceList'])))
                for serverimage in data['getMemberServerImageInstanceListResponse']['memberServerImageInstanceList']:
                    serverimage['customer_id'] = self.customer_id
                    serverimage['datetime'] = self.datetime
                    serverimage['serverImageName'] = serverimage['memberServerImageName']
                    serverimage['originServer'] = ServerList.vpc_server_list(self, serverimage['originalServerInstanceNo']) or ""
                    serverimage['originImage'] = Product.server_image_product_list(self, serverimage['originalServerImageProductCode'])
                    serverimage['imageStatus'] = serverimage['memberServerImageInstanceStatusName']
                    serverimage['blockStorageSize'] = serverimage['memberServerImageBlockStorageTotalSize'] // (1024**3)
                    logging.info(f"{self.customer_id} - {Platform_type} Server Image Name : " + serverimage['memberServerImageName'])
                    result.append(serverimage)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_serverimage_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "serverImageName", "originServer", "originImage", "imageStatus", "blockStorageSize"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                df["blockStorageSize"] = df["blockStorageSize"].astype(int)
                df["originImage"] = df["originImage"].astype(str)
                table_name = f"{self.customer_type}_{Platform_type}_serverimage_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching server image list: {e}")
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
                        {"title": "Service Type:", "value": "Server Image"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result