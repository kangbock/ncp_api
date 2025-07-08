from api.__init__ import init
from util.webhook_util import WebhookUtil
import json
import hashlib
import hmac
import base64
import requests
import time
import logging

class Product(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def server_product_list(self, productCode):
        """
        상품 목록을 조회하는 메서드 <br>
        productCode: 상품 코드 <br>
        return: 상품 목록
        raises ValueError: API 호출 중 오류가 발생한 경우
        """
        rl = ['COM', "KR", "SGN", "JPN", "USWN", "DEN"]
        timestamp = int(time.time() * 1000)
        timestamp = str(timestamp)

        method = "GET"

        access_key = "84CFA93CF97E24413379"
        secret_key = "43B80903779EB78EB0C8F9E8D016C3EA3DB15187"

        for region in rl:
            api_server = "https://billingapi.apigw.ntruss.com"
            uri = "/billing/v1/product/getProductList"
            uri = uri + "?regionCode=" + region
            uri = uri + "&productCode=" + productCode
            uri = uri + "&responseFormatType=json"

            message = method + " " + uri + "\n" + timestamp + "\n" + access_key
            message = bytes(message, 'UTF-8')
            secretkey = bytes(secret_key, 'UTF-8')
            signingKey = base64.b64encode(hmac.new(secretkey, message, digestmod=hashlib.sha256).digest())

            http_header = {
                'Content-type': 'application/json',
                'x-ncp-apigw-timestamp': timestamp,
                'x-ncp-iam-access-key': access_key,
                'x-ncp-apigw-signature-v2': signingKey,
            }

            response = requests.get(api_server + uri, headers=http_header)
            data = json.loads(response.text)
            
            for product in data['getProductListResponse']['productList']:
                result = product['productName']

        return result
    
    def server_image_product_list(self, productCode):
        """
        서버 이미지 상품 목록을 조회하는 메서드 <br>
        productCode: 상품 코드 <br>
        return: 서버 이미지 상품 목록
        raises ValueError: API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        # VPC 서버 이미지 상품 목록을 가져오기 위한 URI
        uri = "/vserver/v2/getServerImageProductList"

        try:
            uri_with_params = uri + f"?productCode={productCode}&responseFormatType=json"
            data = self.transform_uri(uri_with_params, api_server)
            if "Error" in data:
                logging.info(f"{self.customer_id} - Error fetching server image product list: {data['error']}")
                return "Error"  # 오류 발생 시 "Error" 반환
            
            # 상품 코드에 따라 결과를 다르게 설정
            if productCode == "SW.VSVR.OS.LNX64.CNTOS.0703.B050":
                result = "centos-7.3-64"
            elif productCode == "SW.VSVR.OS.LNX64.CNTOS.0708.B050":
                result = "centos-7.8-64"
            elif productCode == "SW.VSVR.BM.OS.LNX64.ORCL.0704":
                result = "oracle-linux-7.4-64"
            elif productCode == "SW.VSVR.OS.LNX64.UBNTU.SVR1804.B050":
                result = "ubuntu-18.04"
            elif productCode == "SW.VSVR.APP.LNX64.CNTOS.0708.WEBTB.LATEST.B050":
                result = "WebtoB-centos-7.8-64"
            elif productCode == "SW.VSVR.OS.LNX64.ROCKY.0806.B050":
                result = "rocky-8.6-64"
            else:
                result = data['getServerImageProductListResponse']['productList'][0]['productName']
                
            return result

        except Exception as e:
            logging.error(f"{self.customer_id} - Error fetching server image product list: {e}")
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
                        {"title": "Product Code:", "value": productCode},
                        {"title": "Error Message:", "value": str(e)},
                    ]
                }
            ]
            WebhookUtil.send_webhook(self, body)

        return "Unknown Error"  # 예외 발생 시 "Unknown Error" 반환