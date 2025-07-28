#import yaml
import json
import os
from colorama import Fore

def load_config_yaml(directory="."):
    """Load the config file from the specified directory."""
    config_path = os.path.join(directory, "config.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def load_config(directory="."):
    """Load the config file from the specified directory."""
    config_path = os.path.join(directory, "config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

# Load JSON file
    with open(config_path, "r") as file:
         #return json.load(file)
         dictconfig = json.load(file)

    dictconfig["sum_output_file"] = f"{dictconfig['project']}_SUMMARY_MEANS"
    dictconfig["day_output_file"] = f"{dictconfig['project']}_DAILY_MEANS"
    if dictconfig["count_prefixes"] == '1h':
       dictconfig["hour_output_file"] = f"{dictconfig['project']}_HOURLY_TRIMMED_MEANS"
    elif dictconfig["count_prefixes"] == '1m':
       dictconfig["hour_output_file"] = f"{dictconfig['project']}_MINUTE_TRIMMED_MEANS"

    dictconfig["time_res_folder"] = f"{dictconfig['count_prefixes']}_level"
    dictconfig["output_file_ext"] = f"{dictconfig['count_prefixes']}_part_proc"

    return dictconfig

def print_message(message):
    print(Fore.GREEN + message + Fore.RESET)


