from api.__init__ import init
from api.server_list import ServerList
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class BlockStorageList(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def classic_block_storage_list(self):
        """
        블록스토리지 목록을 가져오는 메서드 <br>
        return: 블록스토리지 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/server/v2/getBlockStorageInstanceList"

        try:
            result = []  # 각 uri에 대해 result 리스트를 초기화
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
            
            if not len(data['getBlockStorageInstanceListResponse']['blockStorageInstanceList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} block storage count : " + str(len(data['getBlockStorageInstanceListResponse']['blockStorageInstanceList'])))

                for block_storage in data['getBlockStorageInstanceListResponse']['blockStorageInstanceList']:
                    block_storage['customer_id'] = self.customer_id
                    block_storage['datetime'] = self.datetime
                    block_storage['blockStorageSize'] = block_storage['blockStorageSize'] // (1024**3)

                    if block_storage['blockStorageInstanceStatusName'] == 'attached':
                        block_storage['blockServerName'] = block_storage['serverName']
                    elif 'pvc' in block_storage['blockStorageName']:
                        block_storage['blockServerName'] = 'PVC'
                    else:
                        block_storage['blockServerName'] = 'Not Attached'
                        raise ValueError(f"{self.customer_id} - Block Storage {block_storage['blockStorageName']} is not attached to any server")

                    block_storage['zoneCode'] = block_storage['zone']['zoneName']
                    block_storage['blockStorageDiskDetailType'] = block_storage['diskDetailType']['code']

                    logging.info(f"{self.customer_id} - {Platform_type} block storage name : " + block_storage['blockStorageName'])
                    result.append(block_storage)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_block_storage_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "blockServerName", "blockStorageName", "blockStorageSize", "blockStorageInstanceStatusName", "blockStorageDiskDetailType", "zoneCode"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                df["zoneCode"] = df["zoneCode"].astype(str)
                df["blockStorageSize"] = df["blockStorageSize"].astype(int)
                table_name = f"{self.customer_type}_{Platform_type}_block_storage_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching block storage list: {e}")
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
                        {"title": "Service Type:", "value": "Block Storage"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    def vpc_block_storage_list(self):
        """
        블록스토리지 목록을 가져오는 메서드 <br>
        return: 블록스토리지 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/vserver/v2/getBlockStorageInstanceList"

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
            
            if not len(data['getBlockStorageInstanceListResponse']['blockStorageInstanceList']) == 0:
                logging.info(f"{self.customer_id} - {Platform_type} block storage count : " + str(len(data['getBlockStorageInstanceListResponse']['blockStorageInstanceList'])))

                for block_storage in data['getBlockStorageInstanceListResponse']['blockStorageInstanceList']:
                    block_storage['customer_id'] = self.customer_id
                    block_storage['datetime'] = self.datetime
                    block_storage['blockStorageSize'] = block_storage['blockStorageSize'] // (1024**3)

                    if block_storage['blockStorageInstanceStatusName'] == 'attached':
                        block_storage['blockServerName'] = ServerList.vpc_server_list(self, block_storage['serverInstanceNo'])
                    elif 'pvc' in block_storage['blockStorageName']:
                        block_storage['blockServerName'] = 'PVC'
                    else:
                        block_storage['blockServerName'] = 'Not Attached'
                        raise ValueError(f"{self.customer_id} - Block Storage {block_storage['blockStorageName']} is not attached to any server")

                    block_storage['zoneCode'] = block_storage['zoneCode']
                    block_storage['blockStorageDiskDetailType'] = block_storage['blockStorageDiskDetailType']['code']

                    logging.info(f"{self.customer_id} - {Platform_type} block storage name : " + block_storage['blockStorageName'])
                    result.append(block_storage)

                # JSON 파일 경로 생성
                json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_block_storage_list.json"
                JSONUtil.save_to_json(result, json_file_path)

                # DataFrame으로 변환 및 Database에 저장
                columns = ["customer_id", "datetime", "blockServerName", "blockStorageName", "blockStorageSize", "blockStorageInstanceStatusName", "blockStorageDiskDetailType", "zoneCode"]
                df = pd.DataFrame(result, columns=columns)
                df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                df["zoneCode"] = df["zoneCode"].astype(str)
                df["blockStorageSize"] = df["blockStorageSize"].astype(int)
                table_name = f"{self.customer_type}_{Platform_type}_block_storage_list"
                DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching block storage list: {e}")
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
                        {"title": "Service Type:", "value": "Block Storage"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result