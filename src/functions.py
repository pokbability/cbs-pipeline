import os
import hashlib
import hmac

def _mask_generic_str(val):
    if val is None:
        return None
    s = str(val)
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]



def _hash_string(val: str):
    if val is None:
        return None
    salt = (os.getenv("PII_SALT") or "dev_salt_for_local_testing").encode("utf-8")
    return hmac.new(salt, str(val).encode("utf-8"), hashlib.sha256).hexdigest()