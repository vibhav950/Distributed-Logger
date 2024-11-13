import json
from typing import Any
from datetime import datetime
import uuid


__all__ = ["generate_registration_log", "generate_info_log", "generate_warn_log", "generate_error_log", "generate_heartbeat"]


def __NOT_IMPLEMENTED(value) -> Any:
    if isinstance(value, str):
        return ""
    if isinstance(value, int):
        return 0
    if isinstance(value, float):
        return 0.0
    if isinstance(value, list):
        return []
    if isinstance(value, dict):
        return {}
    if isinstance(value, tuple):
        return ()
    if isinstance(value, set):
        return set()
    else:
        return None


def generate_registration_log(node_id: int, service_name: str) -> Any:
    return json.dumps({
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": service_name,
        "timestamp": str(datetime.now())
    })

def generate_info_log(node_id: int, service_name: str, message: str) -> Any:
    return json.dumps({
        "log_id": uuid.uuid4().int,
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": message,
        "service_name": service_name,
        "timestamp": str(datetime.now())
    })

def generate_warn_log(node_id: int, service_name: str, message: str) -> Any:
    return json.dumps({
        "log_id": uuid.uuid4().int,
        "node_id": node_id,
        "log_level": "WARN",
        "message_type": "LOG",
        "message": message,
        "service_name": service_name,
        "response_time_ms": __NOT_IMPLEMENTED("response_time_ms"),
        "threshold_limit_ms": __NOT_IMPLEMENTED("threshold_limit_ms"),
        "timestamp": str(datetime.now())
    })

def generate_error_log(node_id: int, service_name: str, message: str, **kwargs) -> Any:
    return json.dumps({
        "log_id": uuid.uuid4().int,
        "node_id": node_id,
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": message,
        "service_name": service_name,
        "error_details": {
            "error_code": kwargs["error_code"],
            "error_message": kwargs["error_message"]
        },
        "timestamp": str(datetime.now())
    })

def generate_heartbeat(node_id: int, healthy: bool) -> Any:
    return json.dumps({
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status":  "UP" if healthy else "DOWN",
        "timestamp": str(datetime.now())
    })