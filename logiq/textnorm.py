"""Turn a raw log message into a template by masking the variable parts."""

import re

# Any token that contains a digit (ids, latencies, IPs, counts, versions) becomes <*>.
_VARIABLE_TOKEN = re.compile(r"[^\s=:,()\[\]'\"]*\d[^\s=:,()\[\]'\"]*")
_SPACES = re.compile(r"\s+")


def template(message: str) -> str:
    return _SPACES.sub(" ", _VARIABLE_TOKEN.sub("<*>", message)).strip()
