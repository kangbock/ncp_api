from api.__init__ import init
from api.server_list import ServerList
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class NAS(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def classic_nas_list(self):
        """
        NAS 목록을 조회하는 메서드 <br>
        return: NAS 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/server/v2/getNasVolumeInstanceList"

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
            
            if not len(data['getNasVolumeInstanceListResponse']['nasVolumeInstanceList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} NAS Total Number : "  + str(len(data['getNasVolumeInstanceListResponse']['nasVolumeInstanceList'])))

                for nasvolume in data['getNasVolumeInstanceListResponse']['nasVolumeInstanceList']:
                    nasvolume['customer_id'] = self.customer_id
                    nasvolume["datetime"] = self.datetime
                    nasvolume["nasStatus"]= nasvolume["nasVolumeInstanceStatusName"]
                    nasvolume["volumeTotalSize"] = nasvolume["volumeTotalSize"] // (1024**3)
                    nasvolume["volumeSize"] = nasvolume["volumeSize"] // (1024**3)
                    nasvolume["serverName"] = None
                    nasvolume["serverName"] = ", ".join([server["serverName"] for server in nasvolume["nasVolumeServerInstanceList"]])
                    nasvolume["zoneName"] = nasvolume["zone"]["zoneName"]
                    logging.info(f"{self.customer_id} - {Platform_type} NAS Volume Name : " + nasvolume['volumeName'])
                    result.append(nasvolume)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_nas_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "volumeName", "nasStatus", "volumeTotalSize", "volumeSize", "zoneName", "serverName"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                df["volumeTotalSize"] = df["volumeTotalSize"].astype(int)
                df["volumeSize"] = df["volumeSize"].astype(int)
                df["serverName"] = df["serverName"].astype(str)
                table_name = f"{self.customer_type}_{Platform_type}_nas_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching nas list: {e}")
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
                        {"title": "Service Type:", "value": "NAS"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    def vpc_nas_list(self):
        """
        NAS 목록을 조회하는 메서드 <br>
        return: NAS 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/vnas/v2/getNasVolumeInstanceList"

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
            
            if not len(data['getNasVolumeInstanceListResponse']['nasVolumeInstanceList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} NAS Total Number : "  + str(len(data['getNasVolumeInstanceListResponse']['nasVolumeInstanceList'])))

                for nasvolume in data['getNasVolumeInstanceListResponse']['nasVolumeInstanceList']:
                    nasvolume['customer_id'] = self.customer_id
                    nasvolume["datetime"] = self.datetime
                    nasvolume["nasStatus"]= nasvolume["nasVolumeInstanceStatusName"]
                    nasvolume["volumeTotalSize"] = nasvolume["volumeTotalSize"] // (1024**3)
                    nasvolume["volumeSize"] = nasvolume["volumeSize"] // (1024**3)
                    nasvolume["serverName"] = ", ".join([ServerList.vpc_server_list(self, server) for server in nasvolume["nasVolumeServerInstanceNoList"]])
                    nasvolume["zoneName"] = nasvolume["zoneCode"]
                    logging.info(f"{self.customer_id} - {Platform_type} NAS Volume Name : " + nasvolume['volumeName'])
                    result.append(nasvolume)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_nas_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "volumeName", "nasStatus", "volumeTotalSize", "volumeSize", "zoneName", "serverName"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                df["volumeTotalSize"] = df["volumeTotalSize"].astype(int)
                df["volumeSize"] = df["volumeSize"].astype(int)
                df["serverName"] = df["serverName"].astype(str)
                table_name = f"{self.customer_type}_{Platform_type}_nas_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching nas list: {e}")
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
                        {"title": "Service Type:", "value": "NAS"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    