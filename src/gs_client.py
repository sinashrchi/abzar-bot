# src/gs_client.py
"""
Google Sheets DAO for abzar-bot.

Required env (loaded from OS env or project .env):
- GOOGLE_SHEET_ID="...spreadsheet id..."
- GOOGLE_SHEETS_CREDENTIALS="...path to service account JSON..."

Provides:
- read_products()
- read_config_bot()
- read_config_site()
- append_order(row)              # accepts dict (eng keys) or list (values)
- update_order_status(order_no, status, extra=None)
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Load .env from project root robustly
try:
    from dotenv import load_dotenv  # type: ignore

    _ROOT = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=_ROOT / ".env")
except Exception:
    pass

import gspread
from google.oauth2.service_account import Credentials


def _log(event: str, **meta: Any) -> None:
    payload = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": "INFO",
        "event": event,
        "module": "src.gs_client",
        "meta": meta or None,
    }
    print(json.dumps(payload, ensure_ascii=False))


def _authorize() -> gspread.Client:
    creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "./credentials.json")
    if not os.path.isfile(creds_path):
        raise FileNotFoundError(
            "credentials file not found: "
            f"{creds_path} (set GOOGLE_SHEETS_CREDENTIALS to your service-account JSON)"
        )
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    _log("gs_auth", client_email=creds.service_account_email)
    return gspread.authorize(creds)


def open_sheet() -> gspread.Spreadsheet:
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise EnvironmentError("GOOGLE_SHEET_ID is not set")
    gc = _authorize()
    sh = gc.open_by_key(sheet_id)
    _log("gs_open_by_key", sheet_id=sheet_id, title=sh.title)
    return sh


def _open_ws(name: str) -> gspread.Worksheet:
    sh = open_sheet()
    _log("gs_open_ws", name=name)
    return sh.worksheet(name)


def _retrying(fn, *args, **kwargs):
    max_tries = kwargs.pop("_tries", 5)
    base = kwargs.pop("_base", 0.4)
    for i in range(max_tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            if i == max_tries - 1:
                raise
            sleep_s = base * (2**i)
            _log("retry", attempt=i + 1, sleep=sleep_s, error=str(e))
            time.sleep(sleep_s)


# ---------- Canonicalization & header aliases ----------


def _canon(s: str) -> str:
    """Canonicalize a header: strip, remove zero-width/nbsp, lower, '_'->' ',
    cut trailing parenthesis, collapse spaces."""
    if not s:
        return ""
    s = s.strip()
    for ch in ("\u200c", "\u200f", "\u200e", "\xa0"):
        s = s.replace(ch, "")
    s = s.replace("_", " ").lower()
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    s = " ".join(s.split())
    return s


# Canonical keys we use in code
# (for products and for orders/configs)
_CAN_KEYS_PRODUCTS = {
    "code",
    "name",
    "brand",
    "category",
    "short_desc",
    "long_desc",
    "price_retail",
    "price_wholesale_base",
    "min_wholesale_qty",
    "pack_qty",
    "stock",
    "image_url",
    "tags",
    "status",
}

# Map canonical key -> set of alias headers (English/Persian variants)
_PRODUCT_ALIASES: Dict[str, set[str]] = {
    "code": {"code", "کد"},
    "name": {"name", "نام"},
    "brand": {"brand", "برند"},
    "category": {"category", "دسته"},
    "short_desc": {"short desc", "توضیح کوتاه", "توضیح كوتاه"},
    "long_desc": {"long desc", "توضیح بلند", "توضیح كامل", "توضیح کامل"},
    "price_retail": {
        "price retail",
        "قیمت خرده",
        "قیمت خرده فروشی",
        "قیمت خرده‌فروشی",
    },
    "price_wholesale_base": {"price wholesale base", "قیمت عمده پایه"},
    "min_wholesale_qty": {"min wholesale qty", "حداقل مقدار عمده", "حداقل عمده"},
    "pack_qty": {"pack qty", "تعداد در بسته"},
    "stock": {"stock", "موجودی"},
    "image_url": {"image url", "تصویر", "عکس", "تصویر url"},
    "tags": {"tags", "برچسبها", "برچسب‌ها", "برچسب ها"},
    "status": {"status", "وضعیت"},
}

# Build reverse lookup: canon(header text) -> canonical key
_CANON_TO_KEY_PRODUCTS: Dict[str, str] = {}
for k, vals in _PRODUCT_ALIASES.items():
    for v in vals:
        _CANON_TO_KEY_PRODUCTS[_canon(v)] = k


# Aliases for orders worksheet essential columns
_ORDER_ALIASES: Dict[str, set[str]] = {
    "order_no": {"order no", "order", "شماره سفارش", "کد سفارش"},
    "status": {"status", "وضعیت"},
    "extra": {"extra", "یادداشت", "یادداشت ها", "یادداشت‌ها", "notes", "توضیحات"},
    # Optional (used when appending dict rows)
    "created_at": {"created at", "تاریخ ثبت"},
    "customer_name": {"customer name", "نام گیرنده", "نام گیرنده", "نام گیرنده"},
    "phone": {"phone", "موبایل", "تلفن"},
    "items_json": {"items json", "اقلام(json)", "اقلام (json)"},
    "total": {"total", "جمع کل"},
    "payment_method": {"payment method", "روش پرداخت"},
    "receipt_url": {"receipt url", "رسید url"},
    "telegram_id": {"telegram id", "تلگرام id"},
    "address": {"address", "آدرس"},
    "postal_code": {"postal code", "کدپستی", "کد پستی"},
}

_CANON_TO_KEY_ORDERS: Dict[str, str] = {}
for k, vals in _ORDER_ALIASES.items():
    for v in vals:
        _CANON_TO_KEY_ORDERS[_canon(v)] = k


def _normalize_headers(headers: List[str]) -> List[str]:
    """Normalize product headers to canonical keys when possible; else keep canon text."""
    norm: List[str] = []
    for h in headers:
        c = _canon(h or "")
        mapped = _CANON_TO_KEY_PRODUCTS.get(c, c)
        norm.append(mapped)
    return norm


def _row_to_dict(headers: List[str], row: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, h in enumerate(headers):
        if not h:
            continue
        val = row[i] if i < len(row) else ""
        out[h] = val if val != "" else None
    return out


_CACHE: Dict[str, Tuple[float, Any]] = {}
_TTL_PRODUCTS = int(os.getenv("DAO_PRODUCTS_TTL_SEC", "45"))
_TTL_CONFIGS = int(os.getenv("DAO_CONFIGS_TTL_SEC", "45"))


def _get_cache(key: str, ttl: int) -> Optional[Any]:
    now = time.monotonic()
    item = _CACHE.get(key)
    if not item:
        return None
    ts, data = item
    if now - ts > ttl:
        return None
    return data


def _set_cache(key: str, value: Any) -> None:
    _CACHE[key] = (time.monotonic(), value)


# ---------- Products ----------


def read_products() -> List[Dict[str, Any]]:
    cache_key = "products"
    cached = _get_cache(cache_key, _TTL_PRODUCTS)
    if cached is not None:
        return cached

    ws = _open_ws("products")
    rows: List[List[Any]] = _retrying(ws.get_all_values)
    if not rows:
        _log("read_products", count=0)
        _set_cache(cache_key, [])
        return []

    headers_raw = rows[0]
    headers_norm = _normalize_headers(headers_raw)

    data: List[Dict[str, Any]] = []
    for r in rows[1:]:
        data.append(_row_to_dict(headers_norm, r))

    _log("read_products", count=len(data))
    _set_cache(cache_key, data)
    return data


# ---------- Config key/value worksheets ----------


def _read_kv_ws(wsname: str) -> Dict[str, Any]:
    ws = _open_ws(wsname)
    values: List[List[Any]] = _retrying(ws.get_all_values)
    _log(f"read_{wsname}", keys=values[0] if values else [])
    conf: Dict[str, Any] = {}
    for row in values[1:]:
        if not row or len(row) < 2:
            continue
        key = (row[0] or "").strip()
        val = (row[1] or "").strip()
        if not key:
            continue
        if val.startswith("{") or val.startswith("["):
            try:
                conf[key] = json.loads(val)
                continue
            except json.JSONDecodeError:
                pass
        low = val.lower()
        conf[key] = (low == "true") if low in {"true", "false"} else val
    return conf


def read_config_bot() -> Dict[str, Any]:
    cache_key = "config_bot"
    cached = _get_cache(cache_key, _TTL_CONFIGS)
    if cached is not None:
        return cached
    conf = _read_kv_ws("config_bot")
    _set_cache(cache_key, conf)
    return conf


def read_config_site() -> Dict[str, Any]:
    cache_key = "config_site"
    cached = _get_cache(cache_key, _TTL_CONFIGS)
    if cached is not None:
        return cached
    conf = _read_kv_ws("config_site")
    _set_cache(cache_key, conf)
    return conf


# ---------- Orders (append/update) ----------


def _sheet_headers_info(ws: gspread.Worksheet) -> Tuple[List[str], List[str]]:
    headers_raw: List[str] = _retrying(ws.row_values, 1)
    headers_canon = [_canon(h or "") for h in headers_raw]
    return headers_raw, headers_canon


def _find_col_index(headers_raw: List[str], aliases: set[str]) -> Optional[int]:
    """Return 1-based column index for any of alias headers."""
    alias_canon = {_canon(a) for a in aliases}
    for idx, h in enumerate(headers_raw, start=1):
        if _canon(h) in alias_canon:
            return idx
    return None


def append_order(row: Dict[str, Any] | List[Any]) -> int:
    """
    Append one order row.
    If row is dict, it may use English canonical keys (e.g., 'order_no', 'created_at').
    Values are placed in matching sheet columns by header aliasing.
    """
    ws = _open_ws("orders")
    headers_raw, _headers_canon = _sheet_headers_info(ws)

    if isinstance(row, dict):
        ordered: List[Any] = []
        for h in headers_raw:
            c = _canon(h)
            # Try to map sheet header to canonical order key
            key = _CANON_TO_KEY_ORDERS.get(c, "")
            val = ""
            if key and key in row:
                val = row.get(key, "")
            else:
                # fallbacks if user passed exact header text
                val = row.get(h, row.get(c, ""))
            ordered.append(val)
    else:
        # Assume list already in correct order
        ordered = row

    rng = _retrying(ws.append_row, ordered, value_input_option="USER_ENTERED")
    updated_range = (rng or {}).get("updates", {}).get("updatedRange", "")
    _log("append_order", updated_range=updated_range)

    row_idx = 0
    try:
        # e.g., "orders!A10:O10" -> "A10"
        part = updated_range.split("!")[1]
        a1, _ = part.split(":")
        row_idx = int(re.sub(r"[A-Z]+", "", a1))
    except Exception:
        pass
    return row_idx


def update_order_status(
    order_no: str, status: str, extra: Optional[str] = None
) -> bool:
    """
    Update status (and optional 'extra'/notes) for a given order_no.
    Header detection is alias-based and works with Persian/English headers.
    """
    ws = _open_ws("orders")
    rows: List[List[Any]] = _retrying(ws.get_all_values)
    if not rows:
        return False

    headers_raw = rows[0]

    col_order = _find_col_index(headers_raw, _ORDER_ALIASES["order_no"])
    col_status = _find_col_index(headers_raw, _ORDER_ALIASES["status"])
    col_extra = (
        _find_col_index(headers_raw, _ORDER_ALIASES["extra"])
        if extra is not None
        else None
    )

    if col_order is None or col_status is None:
        _log(
            "update_order_status_noheaders", order_col=col_order, status_col=col_status
        )
        return False

    target_row: Optional[int] = None
    needle = (order_no or "").strip()
    for i, r in enumerate(rows[1:], start=2):
        if len(r) >= col_order and (r[col_order - 1] or "").strip() == needle:
            target_row = i
            break
    if not target_row:
        _log("update_order_status_notfound", order_no=order_no)
        return False

    _retrying(ws.update_cell, target_row, col_status, status)
    if extra is not None and col_extra:
        _retrying(ws.update_cell, target_row, col_extra, extra)

    _log("update_order_status", row=target_row, order_no=order_no, status=status)
    return True
