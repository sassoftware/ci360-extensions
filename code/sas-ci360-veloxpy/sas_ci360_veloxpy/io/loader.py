from sas_ci360_veloxpy.config.config_loader import get_config

def getConfigDetails():
        # Fetch configuration details from the ini file
        # Ensure the ini file path is correctly set in the configuration
        
        extapigateway_url = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.extapigateway_url", "")
        client_id = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.client_id", "")
        client_secret = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.client_secret", "")
        api_user_name = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.api_user_name", "")
        api_user_password = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.api_user_password", "")
        return {
             "extapigateway_url" : extapigateway_url,
             "client_id": client_id,
             "client_secret":client_secret,
             "api_user_name":api_user_name,
             "api_user_password":api_user_password}
