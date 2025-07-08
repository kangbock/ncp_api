from api.__init__ import init
from api.server_list import ServerList
from util.json_util import JSONUtil
from util.db_util import DatabaseUtil
from util.webhook_util import WebhookUtil
import pandas as pd
import logging
import datetime

class CloudInsight(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def classic_server_metrics(self):
        """
        서버 메트릭 목록을 조회하는 메서드 <br>
        return: 서버 메트릭 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://cw.apigw.ntruss.com"
            classic_key = "769533687317532672"
        elif self.customer_type == "gov":
            api_server = "https://cw.apigw.gov-ntruss.com"
            classic_key = "769530112713560064"
        else:
            raise ValueError("Invalid customer_type provided")
        
        start_time, end_time, _, _ = self.date_time(self.customer_id) # date_time 메서드 호출
        
        metric_list = [
                       "avg_cpu_used_rto",
                       "avg_fs_usert", 
                       "avg_snd_bps", 
                       "avg_rcv_bps", 
                       "avg_read_byt_cnt",
                       "avg_write_byt_cnt",
                       "max_cpu_used_rto",
                       "max_fs_usert",
                       "max_snd_bps", 
                       "max_rcv_bps", 
                       "max_read_byt_cnt", 
                       "max_write_byt_cnt",
                       "mem_usert",
                       "fs_all_mb",
                       "fs_used_mb",
                       ]

        self.method = "POST"
        uri = "/cw_fea/real/cw/api/data/query/multiple"
        uri_with_params = uri + "?responseFormatType=json"

        # 요청 본문 생성
        body = {
            "metricInfoList": [],
            "timeEnd": end_time,
            "timeStart": start_time
        }

        try:
            server_instance = ServerList.classic_server_list(self)  # 서버 목록 가져오기
            result = [] # 결과 리스트 초기화
            Platform_type = "Classic"
            if self.customer_id == "Covision-atomyprd-weekly":
                interval = "Hour2"
            else:
                interval = "Day1"
            for server in server_instance:
                serverInstanceNo = server["serverInstanceNo"]
                # logging.info(f"{self.customer_id} - Server Instance ID : " + server["serverInstanceNo"])
                serverName = server["serverName"]
                body["metricInfoList"] = [] # 요청 본문 초기화
                for metric in metric_list:
                    if metric == "used_rto" or metric == "mem_usert":
                        # cpu,mem 메트릭에 대해 AVG와 MAX 모두 추가
                        for aggregation in ["AVG", "MAX"]:
                            body["metricInfoList"].append({
                                "aggregation": aggregation,
                                "dimensions": {"instanceNo": serverInstanceNo},
                                "interval": interval,
                                "metric": metric,
                                "cw_key": classic_key,
                            })
                    else:
                        # 다른 메트릭 처리
                        if "avg_" in metric:
                            aggregation = "AVG"
                        elif "max_" in metric:
                            aggregation = "MAX"
                        elif "min_" in metric:
                            aggregation = "MIN"
                        elif "sum_" in metric:
                            aggregation = "SUM"
                        elif "fs_all_mb" or "fs_used_mb" in metric:
                            aggregation = "MAX"
                        else:
                            aggregation = "AVG"

                        body["metricInfoList"].append({
                            "aggregation": aggregation,
                            "dimensions": {"instanceNo": serverInstanceNo},
                            "interval": interval,
                            "metric": metric,
                            "cw_key": classic_key,
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

                # body를 JSON 파일로 저장
                json_file_path = f"./json/{self.customer_id}/request_body.json"
                JSONUtil.save_to_json(body, json_file_path)

                for metric_data in data:
                    metric_name = str(metric_data["metric"])
                    aggregation = metric_data.get("aggregation", "AVG")  # aggregation이 없을 경우 기본값으로 "AVG" 설정
                    for dps in metric_data["dps"]:
                        if metric_name in ["avg_snd_bps", "max_snd_bps", "avg_rcv_bps", "max_rcv_bps",]:
                            value = dps[1] / 8000000  # bit -> MB
                        elif metric_name in ["avg_read_byt_cnt", "max_read_byt_cnt", "avg_write_byt_cnt", "max_write_byt_cnt",]:
                            value = dps[1] / 1000000  # byte -> MB
                        else:
                            value = dps[1]  # 기본 값 (변환 없음)
                        
                        # dps 데이터를 개별적으로 처리
                        dps_entry = {
                            "customer_id": self.customer_id,
                            "datetime": self.datetime,
                            "instanceNo": serverInstanceNo,
                            "serverName": serverName,
                            "metric": metric_name,
                            "aggregation": aggregation,
                            "timestamp": str(datetime.datetime.fromtimestamp(dps[0] // 1000)),
                            "value": value,
                        }

                        # 결과 리스트에 추가
                        result.append(dps_entry)

                        logging_data = f"{serverName} - {metric_name} - {aggregation} - {str(datetime.datetime.fromtimestamp(dps[0] // 1000))} - {value}"
                        logging.info(f"{self.customer_id} - {Platform_type} Server Metric Data: {logging_data}")

            # DataFrame으로 변환 및 Database에 저장
            columns = ["customer_id", "datetime", "instanceNo", "serverName", "metric", "aggregation", "timestamp", "value"]
            df = pd.DataFrame(result, columns=columns)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["value"] = df["value"].astype(float)
            table_name = f"{self.customer_type}_{Platform_type}_metric_list"
            DatabaseUtil().save_to_db(df, table_name)

            # JSON 파일 저장
            json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_metric_list.json"
            JSONUtil.save_to_json(result, json_file_path)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching cloud insight server metric list: {e}")
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
                        {"title": "Service Type:", "value": "Server Metrics"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result
    
    def vpc_server_metrics(self):
        """
        서버 메트릭 목록을 조회하는 메서드 <br>
        return: 서버 메트릭 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://cw.apigw.ntruss.com"
            vpc_key = "460438474722512896"
        elif self.customer_type == "gov":
            api_server = "https://cw.apigw.gov-ntruss.com"
            vpc_key = "567435234753253376"
        else:
            raise ValueError("Invalid customer_type provided")
        
        start_time, end_time, _, _ = self.date_time(self.customer_id) # date_time 메서드 호출
        
        metric_list = [
                       "avg_cpu_used_rto",
                       "avg_fs_usert", 
                       "avg_snd_bps", 
                       "avg_rcv_bps", 
                       "avg_read_byt_cnt",
                       "avg_write_byt_cnt",
                       "max_cpu_used_rto",
                       "max_fs_usert",
                       "max_snd_bps", 
                       "max_rcv_bps", 
                       "max_read_byt_cnt", 
                       "max_write_byt_cnt",
                       "mem_usert",
                       "fs_all_mb",
                       "fs_used_mb",
                       ]

        self.method = "POST"
        uri = "/cw_fea/real/cw/api/data/query/multiple"
        uri_with_params = uri + "?responseFormatType=json"

        # 요청 본문 생성
        body = {
            "metricInfoList": [],
            "timeEnd": end_time,
            "timeStart": start_time
        }

        try:
            server_instance = ServerList.vpc_server_list(self)  # 서버 목록 가져오기
            result = [] # 결과 리스트 초기화
            Platform_type = "VPC"
            if self.customer_id == "Covision-atomyprd-weekly":
                interval = "Hour2"
            else:
                interval = "Day1"
            for server in server_instance:
                serverInstanceNo = server["serverInstanceNo"]
                # logging.info(f"{self.customer_id} - Server Instance ID : " + server["serverInstanceNo"])
                serverName = server["serverName"]
                body["metricInfoList"] = [] # 요청 본문 초기화
                for metric in metric_list:
                    if metric == "used_rto" or metric == "mem_usert":
                        # cpu,mem 메트릭에 대해 AVG와 MAX 모두 추가
                        for aggregation in ["AVG", "MAX"]:
                            body["metricInfoList"].append({
                                "aggregation": aggregation,
                                "dimensions": {"instanceNo": serverInstanceNo},
                                "interval": interval,
                                "metric": metric,
                                "cw_key": vpc_key,
                            })
                    else:
                        # 다른 메트릭 처리
                        if "avg_" in metric:
                            aggregation = "AVG"
                        elif "max_" in metric:
                            aggregation = "MAX"
                        elif "min_" in metric:
                            aggregation = "MIN"
                        elif "sum_" in metric:
                            aggregation = "SUM"
                        elif "fs_all_mb" or "fs_used_mb" in metric:
                            aggregation = "MAX"
                        else:
                            aggregation = "AVG"

                        body["metricInfoList"].append({
                            "aggregation": aggregation,
                            "dimensions": {"instanceNo": serverInstanceNo},
                            "interval": interval,
                            "metric": metric,
                            "cw_key": vpc_key,
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

                # body를 JSON 파일로 저장
                json_file_path = f"./json/{self.customer_id}/request_body.json"
                JSONUtil.save_to_json(body, json_file_path)

                for metric_data in data:
                    metric_name = str(metric_data["metric"])
                    aggregation = metric_data.get("aggregation", "AVG")  # aggregation이 없을 경우 기본값으로 "AVG" 설정
                    for dps in metric_data["dps"]:
                        if metric_name in ["avg_snd_bps", "max_snd_bps", "avg_rcv_bps", "max_rcv_bps",]:
                            value = dps[1] / 8000000  # bit -> MB
                        elif metric_name in ["avg_read_byt_cnt", "max_read_byt_cnt", "avg_write_byt_cnt", "max_write_byt_cnt",]:
                            value = dps[1] / 1000000  # byte -> MB
                        else:
                            value = dps[1]  # 기본 값 (변환 없음)
                        
                        # dps 데이터를 개별적으로 처리
                        dps_entry = {
                            "customer_id": self.customer_id,
                            "datetime": self.datetime,
                            "instanceNo": serverInstanceNo,
                            "serverName": serverName,
                            "metric": metric_name,
                            "aggregation": aggregation,
                            "timestamp": str(datetime.datetime.fromtimestamp(dps[0] // 1000)),
                            "value": value,
                        }

                        # 결과 리스트에 추가
                        result.append(dps_entry)

                        logging_data = f"{serverName} - {metric_name} - {aggregation} - {str(datetime.datetime.fromtimestamp(dps[0] // 1000))} - {value}"
                        logging.info(f"{self.customer_id} - {Platform_type} Server Metric Data: {logging_data}")

            # DataFrame으로 변환 및 Database에 저장
            columns = ["customer_id", "datetime", "instanceNo", "serverName", "metric", "aggregation", "timestamp", "value"]
            df = pd.DataFrame(result, columns=columns)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["value"] = df["value"].astype(float)
            table_name = f"{self.customer_type}_{Platform_type}_metric_list"
            DatabaseUtil().save_to_db(df, table_name)

            # JSON 파일 저장
            json_file_path = f"./json/{self.customer_id}/{self.customer_type}_{Platform_type}_metric_list.json"
            JSONUtil.save_to_json(result, json_file_path)

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching cloud insight server metric list: {e}")
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
                        {"title": "Service Type:", "value": "Server Metrics"},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "API Server:", "value": api_server},
                        {"title": "URI:", "value": uri_with_params}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return result