from api.__init__ import init
from api.mysql_list import MySQL
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging
import datetime

class MySQLMetrics(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)
    
    def mysql_metrics(self):
        """
        """
        if self.customer_type == "pri":
            api_server = "https://cw.apigw.ntruss.com"
            key = "470208511578607616"
        elif self.customer_type == "gov":
            api_server = "https://cw.apigw.gov-ntruss.com"
            key = "577579580831961088"
        else:
            raise ValueError("Invalid customer_type provided")
        
        start_time, end_time, _, _ = self.date_time(self.customer_id) # date_time 메서드 호출
        
        metric_list = ["cpu_pct",
                       "mem_pct",
                       "disk_mysql_used",
                    #    "disk_mysql_free",
                       ]

        self.method = "POST"
        uri = "/cw_fea/real/cw/api/data/query/multiple"
        uri_with_params = uri + "?responseFormatType=json"
        Platform_type = "VPC"

        # 요청 본문 생성
        body = {
            "metricInfoList": [],
            "timeEnd": end_time,
            "timeStart": start_time
        }

        try:
            mysql_instance = MySQL.mysql_list(self)  # MySQL 목록 가져오기
            result = [] # 결과 리스트 초기화
            for mysql in mysql_instance:
                mysqlInstanceNo = mysql["cloudMysqlServerInstanceNo"]
                mysqlName = mysql["serverName"]
                dataStorageSize = mysql["dataStorageSize"]
                body["metricInfoList"] = [] # 요청 본문 초기화
                for metric in metric_list:
                    if "cpu_pct" or "mem_pct" in metric:
                        aggregation = "AVG"
                    elif "disk_mysql_used" in metric:
                        aggregation = "MAX"
                    else:
                        aggregation = "AVG"
                    body["metricInfoList"].append({
                        "aggregation": aggregation,
                        "dimensions": {
                            "instanceNo": mysqlInstanceNo,
                        },
                        "interval": "Day1",
                        "metric": metric,
                        "cw_key": key,
                    })

                # API 요청
                data = self.transform_uri(uri_with_params, api_server, body=body)

                # data 내에 error가 있는지 확인
                if "responseError" in data:
                    error_message = data["responseError"].get("returnMessage", "Unknown error occurred")
                    raise Exception(error_message)
                elif "error" in data:
                    error_message = data["error"].get("message", "Unknown error occurred")
                    raise Exception(error_message)

                # # body를 JSON 파일로 저장
                # json_file_path = f"./json/{self.customer_id}/request_body.json"
                # JSONUtil.save_to_json(body, json_file_path)

                for metric_data in data:
                    metric_name = str(metric_data["metric"])
                    aggregation = metric_data.get("aggregation", "AVG")  # 요청 시 설정한 aggregation 값 가져오기
                    for dps in metric_data["dps"]:
                        if metric_name in ["disk_mysql_used",]:
                            value = dps[1]
                        else:
                            value = dps[1]  # 기본 값 (변환 없음)
                        
                        # dps 데이터를 개별적으로 처리
                        dps_entry = {
                            "customer_id": self.customer_id,
                            "datetime": self.datetime,
                            "instanceNo": mysqlInstanceNo,
                            "serverName": mysqlName,
                            "metric": metric_name,
                            "aggregation": aggregation,
                            "dataStorageSize": dataStorageSize,
                            "timestamp": str(datetime.datetime.fromtimestamp(dps[0] // 1000)),
                            "value": value,
                        }

                        # 결과 리스트에 추가
                        result.append(dps_entry)

                        logging_data = f"{mysqlName} - {metric_name} - {aggregation} - {str(datetime.datetime.fromtimestamp(dps[0] // 1000))} - {value}"
                        logging.info(f"{self.customer_id} - {Platform_type} Mysql Metric Data: {logging_data}")

            # DataFrame으로 변환 및 Database에 저장
            columns = ["customer_id", "datetime", "instanceNo", "serverName", "metric", "aggregation", "dataStorageSize", "timestamp", "value"]
            df = pd.DataFrame(result, columns=columns)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["value"] = df["value"].astype(float)
            table_name = f"{self.customer_type}_{Platform_type}_mysql_metrics"
            DatabaseUtil().save_to_db(df, table_name)

            # JSON 파일 저장
            json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_mysql_metrics.json"
            JSONUtil.save_to_json(result, json_file_path)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching mysql metrics: {e}")
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
                        {"title": "Service Type:", "value": "MySQL Metrics"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result