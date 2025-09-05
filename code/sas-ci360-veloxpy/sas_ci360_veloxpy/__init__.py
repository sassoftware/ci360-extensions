
from .config.config_loader import (
    load_ini,
    load_json,
    make_immutable,
    GLOBAL_CONFIG,
    GLOBAL_API_CONFIG,
)
from .api_interface import APIClient as APIClient

global_config = None
global_api_config = None
tokenData = {}

def initApp(path):
    global tokenData
    global global_config
    global global_api_config
    
    ini_file_path= path
    json_file_path = ".\\sas_ci360_veloxpy\\api.json"
    

    # Load into separate containers
    make_immutable(load_ini(ini_file_path, name="ini_file_path", container="GLOBAL_CONFIG"))
    make_immutable(load_json(json_file_path, name="json_file_path", container="GLOBAL_API_CONFIG"))

