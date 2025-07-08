from api.__init__ import init
from util.webhook_util import WebhookUtil
import pyodbc
import os
import logging

class DatabaseUtil:
    def __init__(self):
        """
        초기화 메서드 <br>
        customer_id: 고객 ID (선택적) <br>
        customer_type: 고객 유형 (선택적) <br>
        access_key: 접근 키 (선택적) <br>
        secret_key: 비밀 키 (선택적) <br>
        """

        # 데이터베이스 연결 초기화
        server = os.getenv("DATABASE_SERVER")
        database = os.getenv("DATABASE_NAME")
        username = os.getenv("DATABASE_USER")
        password = os.getenv("DATABASE_PWD")
        if not server or not database or not username or not password:
            raise ValueError("DATABASE 환경 변수가 설정되지 않았습니다.")
        
        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        self.connection = pyodbc.connect(self.connection_string)

    def save_to_db(self, df, table_name):
        """
        DataFrame을 데이터베이스에 저장하는 메서드 <br>
        df: 저장할 DataFrame <br>
        table_name: 저장할 테이블 이름
        """
        
        # 데이터프레임가 없을 경우 저장하지 않음
        if df.empty:
            return
        
        # 데이터베이스와 상호작용하기 위해 커서를 생성
        cursor = self.connection.cursor()
        try:
            # for index, row in df.iterrows():
            #     cursor.execute(
            #         f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})",
            #         *tuple(row)
            #     )
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})"
            data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
            cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Failed to save data to database: {e}")
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
                        {"title": "Customer ID:", "value": df['customer_id'][0]},
                        {"title": "Platform Type:", "value": "VPC" if "VPC" in table_name else "Classic" if "Classic" in table_name else "Unknown"},
                        {"title": "Table Name:", "value": table_name},
                        {"title": "Error Message:", "value": str(e)},
                        {"title": "Datetime:", "value": str(df['datetime'].iloc[0]) if not df.empty else "N/A"}
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)
        finally:
            cursor.close()


