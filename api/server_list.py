from api.__init__ import init
from api.product import Product
from api.vpc_list import VPC
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class ServerList(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def classic_server_list(self, serverInstanceNo=None):
        """
        서버 목록을 가져오는 메서드 <br>
        return: 서버 목록 (딕셔너리 형식)
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/server/v2/getServerInstanceList"

        try:
            result = []
            Platform_type = 'Classic'
            if serverInstanceNo is None:
                uri_with_params = uri + "?responseFormatType=json"
                # API 요청
                data = self.transform_uri(uri_with_params, api_server)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)
                
                if not len(data['getServerInstanceListResponse']['serverInstanceList']) == 0:
                    logging.info(f"{self.customer_id} - {Platform_type} Server Total Number : " + str(len(data['getServerInstanceListResponse']['serverInstanceList'])))
                    for server in data['getServerInstanceListResponse']['serverInstanceList']:
                        server['customer_id'] = self.customer_id
                        server['datetime'] = self.datetime
                        # ProductCode에 대한 상품명 조회
                        server['serverSpec'] = Product.server_product_list(self, server['serverProductCode'])
                        server['serverStatus'] = server['serverInstanceStatusName']
                        server['platformType'] = server['platformType']['codeName']
                        server['vpcName'] = "Classic"
                        server['zoneCode'] = server['zone']['zoneName']
                        server['serverImage'] = server['serverImageName']
                        logging.info(f"{self.customer_id} - {Platform_type} Server Name : " + server['serverName'])
                        result.append(server)

                    # JSON 파일 경로 생성
                    json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_server_list.json"

                    # DataFrame으로 변환 및 Database에 저장
                    columns = ["customer_id", "datetime", "serverName", "serverImage", "platformType", "publicIp", "serverSpec", "vpcName", "zoneCode", "serverStatus"]
                    df = pd.DataFrame(result, columns=columns)
                    df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                    df["zoneCode"] = df["zoneCode"].astype(str)
                    df["serverName"] = df["serverName"].astype(str)
                    table_name = f"{self.customer_type}_{Platform_type}_server_list"
                    JSONUtil.save_to_json(result, json_file_path)
                    DatabaseUtil().save_to_db(df, table_name)

                    # logging.info("raw data :" + str(result))
            else:
                uri_with_params = uri + f"?serverInstanceNoList.1={serverInstanceNo}&responseFormatType=json"
                data = self.transform_uri(uri_with_params, api_server)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)
                
                for server in data['getServerInstanceListResponse']['serverInstanceList']:
                    if server['serverInstanceNo'] == serverInstanceNo:
                        server_name = server['serverName']
                        return server_name
                    else:
                        logging.error(f"{self.customer_id} - No server found with instance number {serverInstanceNo}")
                        return None

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching server list: {e}")
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
                        {"title": "Service Type:", "value": "Server"},
                        {"title": "Server Instance No:", "value": serverInstanceNo if serverInstanceNo else "N/A"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params},
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    def vpc_server_list(self, serverInstanceNo=None):
        """
        서버 목록을 가져오는 메서드 <br>
        return: 서버 목록 (딕셔너리 형식)
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")
        
        uri = "/vserver/v2/getServerInstanceList"

        try:
            result = []
            Platform_type = 'VPC'
            if serverInstanceNo is None:
                uri_with_params = uri + "?responseFormatType=json"
                # API 요청
                data = self.transform_uri(uri_with_params, api_server)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)
                
                if not len(data['getServerInstanceListResponse']['serverInstanceList']) == 0:
                    logging.info(f"{self.customer_id} - {Platform_type} Server Total Number : " + str(len(data['getServerInstanceListResponse']['serverInstanceList'])))
                    for server in data['getServerInstanceListResponse']['serverInstanceList']:
                        server['customer_id'] = self.customer_id
                        server['datetime'] = self.datetime
                        # ProductCode에 대한 상품명 조회
                        server['serverSpec'] = Product.server_product_list(self, server['serverProductCode'])
                        server['serverStatus'] = server['serverInstanceStatusName']
                        server['platformType'] = server['platformType']['codeName']
                        server['vpcName'] = VPC.vpc_list(self, server['vpcNo'], server['regionCode'])
                        server['serverImage'] = Product.server_image_product_list(self, server['serverImageProductCode'])
                        server['zoneCode'] = server['zoneCode']
                        logging.info(f"{self.customer_id} - {Platform_type} Server Name : " + server['serverName'])
                        result.append(server)

                    # JSON 파일 경로 생성
                    json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_server_list.json"

                    # DataFrame으로 변환 및 Database에 저장
                    columns = ["customer_id", "datetime", "serverName", "serverImage", "platformType", "publicIp", "serverSpec", "vpcName", "zoneCode", "serverStatus"]
                    df = pd.DataFrame(result, columns=columns)
                    df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
                    df["zoneCode"] = df["zoneCode"].astype(str)
                    df["serverName"] = df["serverName"].astype(str)
                    table_name = f"{self.customer_type}_{Platform_type}_server_list"
                    JSONUtil.save_to_json(result, json_file_path)
                    DatabaseUtil().save_to_db(df, table_name)

                # logging.info("raw data :" + str(result))
            else:
                uri_with_params = uri + f"?serverInstanceNoList.1={serverInstanceNo}&responseFormatType=json"
                data = self.transform_uri(uri_with_params, api_server)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)
                
                for server in data['getServerInstanceListResponse']['serverInstanceList']:
                    if server['serverInstanceNo'] == serverInstanceNo:
                        server_name = server['serverName']
                        return server_name
                    else:
                        logging.error(f"{self.customer_id} - No server found with instance number {serverInstanceNo}")
                        return None

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching server list: {e}")
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
                        {"title": "Service Type:", "value": "Server"},
                        {"title": "Server Instance No:", "value": serverInstanceNo if serverInstanceNo else "N/A"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params},
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result