from api.__init__ import init
from api.nas_list import NAS
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging

class NASMetrics(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def classic_nas_metrics(self):
        """
        Classic NAS 사용량 데이터를 조회하는 메서드
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/server/v2/getNasVolumeInstanceRatingList"
        startTime, endTime = self.nas_date_time()  # date_time 메서드 호출

        try:
            result = []
            Platform_type = 'Classic'
            nas_instance = NAS.classic_nas_list(self)
            for nas in nas_instance:
                nasVolumeInstanceNo = nas['nasVolumeInstanceNo']
                interval = "1d"
                uri_with_params = uri + f"?nasVolumeInstanceNo={nasVolumeInstanceNo}&startTime={startTime}&endTime={endTime}&interval={interval}&responseFormatType=json"
                data = self.transform_uri(uri_with_params, api_server)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)
                
                if not len(data['getNasVolumeInstanceRatingListResponse']['nasVolumeInstanceRatingList']) == 0:
                    for nasvolume in data['getNasVolumeInstanceRatingListResponse']['nasVolumeInstanceRatingList']:
                        nasvolume['customer_id'] = self.customer_id
                        nasvolume["datetime"] = self.datetime
                        nasvolume["volumeName"] = nas["volumeName"]
                        nasvolume["volumeTotalSize"] = nas["volumeTotalSize"]
                        nasvolume["volumeSize"] = nas["volumeSize"]
                        nasvolume["maxVolumeUseSize"] = nasvolume["maxVolumeUseSize"] // (1024**3)
                        logging.info(f"{self.customer_id} - {Platform_type} NAS Metric Data : volumeName - "  + str(nasvolume["volumeName"]) + ", volumeTotalSize - " + str(nasvolume["volumeTotalSize"]) + "GB, volumeSize - " + str(nasvolume["volumeSize"]) + "GB, maxVolumeUseSize - " + str(nasvolume["maxVolumeUseSize"]) + "GB, maxVolumeUseRatio - " + str(nasvolume["maxVolumeUseRatio"]) + "%")
                        result.append(nasvolume)

            # JSON 파일 경로 생성
            json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_nas_metric.json"
            JSONUtil.save_to_json(result, json_file_path)

            # DataFrame으로 변환 및 Database에 저장
            columns = ["customer_id", "datetime", "volumeName", "volumeTotalSize", "volumeSize", "maxVolumeUseSize", "maxVolumeUseRatio"]
            df = pd.DataFrame(result, columns=columns)
            df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
            df["volumeTotalSize"] = df["volumeTotalSize"].astype(int)
            df["volumeSize"] = df["volumeSize"].astype(int)
            df["maxVolumeUseSize"] = df["maxVolumeUseSize"].astype(int)
            df["maxVolumeUseRatio"] = df["maxVolumeUseRatio"].astype(float)
            table_name = f"{self.customer_type}_{Platform_type}_nas_metric"
            DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching nas metrics: {e}")
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
                        {"title": "Service Type:", "value": "NAS Metrics"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    def vpc_nas_metrics(self):
        """
        VPC NAS 사용량 데이터를 조회하는 메서드
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/vnas/v2/getNasVolumeInstanceRatingList"
        startTime, endTime = self.nas_date_time()  # date_time 메서드 호출

        try:
            result = []
            Platform_type = 'VPC'
            nas_instance = NAS.vpc_nas_list(self)
            for nas in nas_instance:
                nasVolumeInstanceNo = nas['nasVolumeInstanceNo']
                interval = "1d"
                uri_with_params = uri + f"?nasVolumeInstanceNo={nasVolumeInstanceNo}&startTime={startTime}&endTime={endTime}&interval={interval}&responseFormatType=json"
                data = self.transform_uri(uri_with_params, api_server)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)
                
                if not len(data['getNasVolumeInstanceRatingListResponse']['nasVolumeInstanceRatingList']) == 0:
                    for nasvolume in data['getNasVolumeInstanceRatingListResponse']['nasVolumeInstanceRatingList']:
                        nasvolume['customer_id'] = self.customer_id
                        nasvolume["datetime"] = self.datetime
                        nasvolume["volumeName"] = nas["volumeName"]
                        nasvolume["volumeTotalSize"] = nas["volumeTotalSize"]
                        nasvolume["volumeSize"] = nas["volumeSize"]
                        nasvolume["maxVolumeUseSize"] = nasvolume["maxVolumeUseSize"] // (1024**3)
                        logging.info(f"{self.customer_id} - {Platform_type} NAS Metric Data : volumeName - "  + str(nasvolume["volumeName"]) + ", volumeTotalSize - " + str(nasvolume["volumeTotalSize"]) + "GB, volumeSize - " + str(nasvolume["volumeSize"]) + "GB, maxVolumeUseSize - " + str(nasvolume["maxVolumeUseSize"]) + "GB, maxVolumeUseRatio - " + str(nasvolume["maxVolumeUseRatio"]) + "%")
                        result.append(nasvolume)

            # JSON 파일 경로 생성
            json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_nas_metric.json"
            JSONUtil.save_to_json(result, json_file_path)

            # DataFrame으로 변환 및 Database에 저장
            columns = ["customer_id", "datetime", "volumeName", "volumeTotalSize", "volumeSize", "maxVolumeUseSize", "maxVolumeUseRatio"]
            df = pd.DataFrame(result, columns=columns)
            df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
            df["volumeTotalSize"] = df["volumeTotalSize"].astype(int)
            df["volumeSize"] = df["volumeSize"].astype(int)
            df["maxVolumeUseSize"] = df["maxVolumeUseSize"].astype(int)
            df["maxVolumeUseRatio"] = df["maxVolumeUseRatio"].astype(float)
            table_name = f"{self.customer_type}_{Platform_type}_nas_metric"
            DatabaseUtil().save_to_db(df, table_name)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching nas metrics: {e}")
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
                        {"title": "Service Type:", "value": "NAS Metrics"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": self.datetime},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result