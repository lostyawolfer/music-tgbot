import os
from dotenv import dotenv_values

# Load .env (if exists) and merge with real environment variables.
# Environment variables take precedence over .env values.
_dotenv = dotenv_values('.env')
configfile = {**_dotenv, **dict(os.environ)}

def env_bool(key: str, default: bool = False) -> bool:
    v = configfile.get(key, None)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default

def env_int(key: str, default: int = 0) -> int:
    v = configfile.get(key, None)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default

def env_str(key: str, default: str = "") -> str:
    v = configfile.get(key, None)
    if v is None:
        return default
    return str(v).strip()
