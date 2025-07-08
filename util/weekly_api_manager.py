from api.__init__ import init
from api.all_resource_list import AllResourceList
from api.server_image_list import ServerImageList
from api.blockstorage_list import BlockStorageList
from api.public_ip_list import PublicIP
from api.loadbalancer_list import LoadBalancerList
from api.postgresql_list import PostgreSQL
from api.mysql_list import MySQL
from api.redis_list import Redis
from api.mssql_list import MSSQL
from api.mongodb_list import MongoDB
from api.clouddb_list import CloudDB
from api.nas_metrics import NASMetrics
from api.server_metrics import CloudInsight
from api.mysql_metrics import MySQLMetrics
import logging
import datetime

class APIManager:

    def api_manager(self, data):
        """
        각 API를 호출하는 메서드 <br>
        data: 고객 정보 (customer_id, customer_type, access_key, secret_key) <br>
        return: None
        """
        # logging.info("Azure Key Vault Name : " + os.environ["KEY_VAULT_NAME"])
        # logging.info("Azure SQL Server URL : " + os.environ["DATABASE_SERVER"])
        # logging.info("Azure SQL Database : " + os.environ["DATABASE_NAME"])
        # logging.info("Azure Database User : " + os.environ["DATABASE_USER"])
        # logging.info("Azure Database Password : " + "".join(["*" for _ in os.environ["DATABASE_PWD"]]))

        _, _, str_start_time, str_end_time = CloudInsight.date_time(self) # date_time 메서드 호출
        logging.info(f"NCP Weekly Report v2 - Start Time: {str_start_time}, End Time: {str_end_time}")
        timenow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        # 특정 고객사와 리소스
        included_functions = {
            "CUSTOEMR_ID_WEEKLY": [
                "api.server_metrics.CloudInsight.vpc_server_metrics",
                "api.blockstorage_list.BlockStorageList.vpc_block_storage_list",
                "api.nas_metrics.NASMetrics.vpc_nas_metrics",
                "api.mysql_metrics.MySQLMetrics.mysql_metrics",
            ],
        }

        for row in data:
            customer_id, customer_type, access_key, secret_key = row

            # logging.info("customer_id : " + customer_id)
            # logging.info("customer_type : " + customer_type)
            # logging.info("access_key : " + access_key)
            # logging.info("secret_key : " + "".join(['*' for _ in range(len(secret_key))]))

            api_instance = init(customer_id, customer_type, access_key, secret_key, timenow)  # Assuming this setup

            # 모든 함수 호출 리스트
            api_functions = [
                AllResourceList.all_resource_list,
                ServerImageList.classic_serverimage_list,
                ServerImageList.vpc_serverimage_list,
                BlockStorageList.classic_block_storage_list,
                BlockStorageList.vpc_block_storage_list,
                PublicIP.classic_public_ip_list,
                PublicIP.vpc_public_ip_list,
                LoadBalancerList.classic_loadbalancer_list,
                LoadBalancerList.vpc_loadbalancer_list,
                PostgreSQL.postgresql_list,
                MySQL.mysql_list,
                MSSQL.mssql_list,
                MongoDB.mongodb_list,
                Redis.redis_list,
                CloudDB.clouddb_list,
                CloudInsight.classic_server_metrics,
                CloudInsight.vpc_server_metrics,
                NASMetrics.classic_nas_metrics,
                NASMetrics.vpc_nas_metrics,
                MySQLMetrics.mysql_metrics,
            ]

            # 실행할 특정 고객사와 리소스 필터링 (평소엔 주석처리)
            included_function_names = included_functions.get(customer_id, [])
            filtered_functions = filter(
                lambda func: f"{func.__module__}.{func.__qualname__}" in included_function_names,
                api_functions
            )

            # 필터링된 함수 호출
            for func in filtered_functions:
                try:
                    func(api_instance)
                except Exception as e:
                    logging.error(f"Error executing {func.__name__} for customer_id {customer_id}: {e}")