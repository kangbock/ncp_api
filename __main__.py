from util.api_manager import APIManager
from util.key_vault import KeyVaultManager

def main():
    """
    Main method to fetch data from Key Vault and call the APIManager
    """
    # Create an instance of KeyVaultManager
    manager = KeyVaultManager()
    
    try:
        # Fetch data
        data = manager.fetch_secrets_data()
        APIManager().api_manager(data)

    except Exception or ValueError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()