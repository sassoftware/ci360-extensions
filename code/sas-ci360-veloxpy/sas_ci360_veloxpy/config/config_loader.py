import os
import json
from configparser import ConfigParser
from types import MappingProxyType

# Global containers
GLOBAL_CONFIG = {}
GLOBAL_API_CONFIG = {}

# Helper to get target container
def _get_container(container_name: str) -> dict:
    return {
        "GLOBAL_CONFIG": GLOBAL_CONFIG,
        "GLOBAL_API_CONFIG": GLOBAL_API_CONFIG,
    }.get(container_name, GLOBAL_CONFIG)


def load_ini(path: str, name: str = None, container: str = "GLOBAL_CONFIG"):
    if not os.path.isfile(path):
        raise FileNotFoundError(f"INI file not found: {path}")
    
    parser = ConfigParser()
    parser.read(path)
    _get_container(container)[name or path] = parser


def load_json(path: str, name: str = None, container: str = "GLOBAL_API_CONFIG"):
    if not os.path.isfile(path):
        raise FileNotFoundError(f"JSON file not found: {path}")
    
    with open(path, "r") as f:
        data = json.load(f)
    _get_container(container)[name or path] = data

def make_immutable(mapping):
    if mapping is None:
        return MappingProxyType({})  # or raise an error, depending on your use case
    if not isinstance(mapping, dict):
        raise TypeError("Expected a dict for MappingProxyType")
    return MappingProxyType(mapping)

def deep_clone(obj):
    import copy
    return copy.deepcopy(obj)

def get_config(container: str, name: str, dotted_key: str, default=None):

    config_obj = _get_container(container).get(name)
    
    if config_obj is None:
        return default

    if isinstance(config_obj, ConfigParser):
        try:
            section, key = dotted_key.split(".", 1)
            return config_obj.get(section, key)
        except Exception:
            return default

    elif isinstance(config_obj, dict):
        try:
            keys = dotted_key.split(".")
            val = config_obj
            for k in keys:
                val = val[k]
            return val
        except Exception:
            return default

    return default


def replace_path_params(path: str, params: dict) -> str:
        """
        Replace placeholders in the path string with values from params.

        Parameters
        ----------
        path : str
            The URL path containing placeholders like {param_name}.
        params : dict
            Dictionary of parameter names and their values.

        Returns
        -------
        str
            The path with placeholders replaced by actual values.
        """
        for key, value in params.items():
            placeholder = "{" + key + "}"
            path = path.replace(placeholder, str(value))
        return path

