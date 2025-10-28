def normalize_fa(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    # ی/ك → ی/ک
    s = s.replace("ي", "ی").replace("ك", "ک")
    # نیم‌فاصله و علائم رایج
    for ch in ["‌", "\u200c", ",", "،"]:
        s = s.replace(ch, " ")
    # فاصله‌های اضافه
    while "  " in s:
        s = s.replace("  ", " ")
    return s.lower()
