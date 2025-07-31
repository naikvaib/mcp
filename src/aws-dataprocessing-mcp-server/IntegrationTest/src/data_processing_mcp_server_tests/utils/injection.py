import json, re
from typing import Any

def extract_path(response: dict, path: str) -> Any:
    tokens = re.findall(r"\w+|\[\d+\]", path)
    value = response
    for token in tokens:
        if re.fullmatch(r"\[\d+\]", token):
            index = int(token[1:-1])
            if not isinstance(value, list):
                raise ValueError(f"Expected list at {token}, got {type(value)}")
            value = value[index]
        else:
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except Exception:
                    raise ValueError(f"Expected JSON string at '{token}' but failed to parse.")
            if not isinstance(value, dict) or token not in value:
                raise ValueError(f"Cannot find key '{token}' in {value}")
            value = value[token]
    return value
