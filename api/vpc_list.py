from api.__init__ import init

class VPC(init):
    def __init__(self, customer_id, customer_type, access_key, secret_key):
        super().__init__(customer_id, customer_type, access_key, secret_key)

    def vpc_list(self, vpcNo, regioncode):
        """
        VPC 목록을 조회하는 메서드 <br>
        vpcNo: VPC 번호 <br>
        regioncode: 리전 코드 <br>
        return: VPC 목록 (딕셔너리 형식) <br>
        raises ValueError: 고객 유형이 잘못되었거나 API 호출 중 오류가 발생한 경우
        """
        if self.customer_type == "pri":
            api_server = "https://ncloud.apigw.ntruss.com"
        elif self.customer_type == "gov":
            api_server = "https://ncloud.apigw.gov-ntruss.com"
        else:
            raise ValueError("Invalid customer_type provided")

        uri = "/vpc/v2/getVpcList"
        uri = uri + "?vpcNo=" + vpcNo
        uri = uri + "&regionCode=" + regioncode
        uri = uri + "&responseFormatType=json"

        data = init.transform_uri(self, uri, api_server)

        for vpc in data['getVpcListResponse']['vpcList']:
            result = vpc['vpcName']

        return result