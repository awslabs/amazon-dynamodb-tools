import boto3
import json

from ddbtools import constants
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    """Convert the Decimal type to a string for display in JSON data"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return f"{obj:.2f}"
        return json.JSONEncoder.default(self, obj)

        