import os,sys
import json
from decimal import Decimal

def convert_to_decimal(obj):
    if isinstance(obj, dict):
        # If the object is a dictionary, convert its values
        return {key: convert_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        # If the object is a list, convert its elements
        return [convert_to_decimal(element) for element in obj]
    elif isinstance(obj, str):
        # If the object is a string, try to convert it to a Decimal
        try:
            return Decimal(obj)
        except:
            return obj
    else:
        # If the object is anything else, return it unchanged
        return obj


def check_dir(path):
    if(not(os.path.exists(path))):
        os.makedirs(path)
