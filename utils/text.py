def safe_join(parts, sep=" "):
    return sep.join([p for p in parts if p]).strip()
