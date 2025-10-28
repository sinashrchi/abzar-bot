import datetime as dt
import json
import os
import sys

import gspread
from dotenv import load_dotenv

load_dotenv()

problems: list[str] = []


def ok(msg: str):
    print(f"[PASS] {msg}")


def fail(msg: str, tag: str | None = None):
    print(f"[FAIL] {msg}")
    if tag:
        problems.append(tag)


# 1) مسیر فایل سرویس‌اکانت و شناسه شیت از .env
cred_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "./credentials.json")
sheet_id = (os.getenv("GOOGLE_SHEET_ID") or "").strip()

# 1.a) اعتبارسنجی فایل credentials.json
client_email = ""
if os.path.exists(cred_path):
    ok(f"credentials.json found at {cred_path}")
    try:
        with open(cred_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        client_email = data.get("client_email", "")
        print(f"[INFO] service account: {client_email}")
    except Exception as e:
        fail(f"cannot read credentials.json: {e}", "read_credentials")
else:
    fail(f"credentials.json missing at {cred_path}", "cred_missing")

# 1.b) اعتبارسنجی متغیر GOOGLE_SHEET_ID
if sheet_id:
    ok(f"GOOGLE_SHEET_ID present: {sheet_id[:6]}...{sheet_id[-6:]}")
else:
    fail("GOOGLE_SHEET_ID missing in .env", "sheet_id_missing")

# اگر پایه‌ها مشکل دارند، ادامه ندهیم
if problems:
    print("\nSummary: FAIL (pre-checks)")
    sys.exit(2)

# 2) اتصال به Google Sheets
try:
    gc = gspread.service_account(filename=cred_path)
    sh = gc.open_by_key(sheet_id)
    ok(f"opened spreadsheet: {sh.title}")

    titles = [ws.title for ws in sh.worksheets()]
    print("[INFO] tabs:", ", ".join(titles))

    required = [
        "products",
        "orders",
        "customers_b2b",
        "pricing_rules",
        "broadcasts",
        "config_bot",
        "config_site",
        "logs",
    ]
    missing = [t for t in required if t not in titles]
    if missing:
        fail(f"missing tabs: {missing}", "missing_tabs")
    else:
        ok("all required tabs exist")

    # 3) درج یک ردیف آزمون در logs
    try:
        ws = sh.worksheet("logs")
        now = dt.datetime.now().isoformat(timespec="seconds")
        row = [
            now,
            "INFO",
            "phase2_verify",
            client_email or "-",
            "append_test",
            json.dumps({"ok": True}, ensure_ascii=False),
        ]
        ws.append_row(row, value_input_option="RAW")
        ok("append test row to 'logs'")
    except Exception as e:
        fail(f"cannot append to 'logs': {e}", "append_logs")

except Exception as e:
    fail(f"cannot open spreadsheet or auth failed: {e}", "open_sheet")

# 4) نتیجه نهایی + کد خروج
if problems:
    print("\nSummary: FAIL")
    for p in problems:
        print("-", p)
    sys.exit(1)
else:
    print("\nSummary: ALL PASS")
    sys.exit(0)
