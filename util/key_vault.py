from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import HttpResponseError
import os
import logging

class KeyVaultManager:
    def __init__(self, key_vault_name=None):
        """
        초기화 메서드 <br>
        key_vault_name: Key Vault 이름 (기본값: 환경 변수 "KEY_VAULT_NAME")
        """
        if key_vault_name is None:
            key_vault_name = os.environ["KEY_VAULT_NAME"]
        self.key_vault_name = key_vault_name
        self.key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
        # Authenticate and initialize client
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=self.key_vault_url, credential=self.credential)
        # Define a list of tag keys you want to look up
        self.tag_keys_to_lookup = ["customer_type", "access_key"]  # Add more tag keys as needed

    def fetch_secrets_data(self):
        """
        Key Vault에서 비밀 데이터를 가져오는 메서드 <br>
        return: 비밀 데이터 목록 (리스트 형식)
        """
        # List all secrets
        secrets = self.client.list_properties_of_secrets()

        # Prepare data storage
        data = []

        # 비밀 이름 바꿔야하고 태그에 access key 추가
        for secret in secrets:
            customer_id = secret.name
            try:
                secret_key = self.client.get_secret(customer_id).value  # Retrieve secret value
            except HttpResponseError as e:
                if "SecretDisabled" in str(e):
                    logging.warning(f"{customer_id} - Secret is disabled, skipping.")
                    continue
                else:
                    raise e

            tags = secret.tags if secret.tags else {}  # Extract tags dictionary

            # Get values for each tag key in tag_keys_to_lookup
            tag_values = {}
            for tag_key in self.tag_keys_to_lookup:
                tag_values[tag_key] = tags.get(tag_key, "Tag Not Found")

            # Prepare a row with secret name, each tag value, and the secret value
            row = [customer_id] + [tag_values[key] for key in self.tag_keys_to_lookup] + [secret_key]
            data.append(row)
        
        return data



