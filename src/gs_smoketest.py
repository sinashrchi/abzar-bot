# src/gs_smoketest.py
"""
Smoke test for gs_client DAO.
Run with:  python -m src.gs_smoketest
"""

from __future__ import annotations

import json
import random
import re
import time
from typing import Dict, List, Optional

from .gs_client import _open_ws  # Ø§Ø³ØªÙØ§Ø¯Ù‡ ØµØ±ÙØ§Ù‹ Ø¨Ø±Ø§ÛŒ Ø®ÙˆØ§Ù†Ø¯Ù† Ù‡Ø¯Ø±Ù‡Ø§ÛŒ ÙˆØ§Ù‚Ø¹ÛŒ Ø´ÛŒØª
from .gs_client import (
    append_order,
    read_config_bot,
    read_config_site,
    read_products,
    update_order_status,
)


def _p(obj) -> None:
    """Pretty print JSON with UTF-8."""
    print(json.dumps(obj, ensure_ascii=False))


def _canon(s: str) -> str:
    """
    Ù†Ø±Ù…Ø§Ù„â€ŒØ³Ø§Ø²ÛŒ Ø§Ø³Ù… Ø³ØªÙˆÙ†â€ŒÙ‡Ø§:
    - Ø­Ø°Ù Ø²ÛŒØ±Ø®Ø· (_) Ùˆ Ù†ÛŒÙ…â€ŒÙØ§ØµÙ„Ù‡ (ZWJ)
    - Ø­Ø°Ù Ù‡Ø± Ú†ÛŒØ²ÛŒ Ø¯Ø§Ø®Ù„ Ù¾Ø±Ø§Ù†ØªØ² Ù…Ø«Ù„ (NEW/...)
    - Ú©ÙˆÚ†Ú©â€ŒØ³Ø§Ø²ÛŒ Ùˆ ØªÚ©â€ŒÙØ§ØµÙ„Ù‡â€ŒÚ©Ø±Ø¯Ù†
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("_", " ").replace("\u200c", "")
    s = re.sub(r"\(.*?\)", "", s)  # remove any (...) like (NEW/...)
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_header_map(headers: List[str]) -> Dict[str, str]:
    """
    Ø§Ø² Ù„ÛŒØ³Øª Ù‡Ø¯Ø±Ù‡Ø§ÛŒ Ø±Ø¯ÛŒÙ Û± Ø´ÛŒØªØŒ ÛŒÚ© Ù…Ù¾ canonical->actual Ù…ÛŒâ€ŒØ³Ø§Ø²Ø¯.
    Ø¨Ø±Ø§ÛŒ Ú©Ù„ÛŒØ¯Ù‡Ø§ÛŒ Ù…ÙˆØ±Ø¯Ù†ÛŒØ§Ø²ØŒ Ù…Ø¹Ø§Ø¯Ù„ ÙˆØ§Ù‚Ø¹ÛŒ Ø³ØªÙˆÙ† Ø±Ø§ Ù¾ÛŒØ¯Ø§ Ù…ÛŒâ€ŒÚ©Ù†ÛŒÙ….
    """
    canon_to_actual: Dict[str, str] = {}
    for h in headers:
        canon_to_actual[_canon(h)] = h
    return canon_to_actual


def _pick(header_map: Dict[str, str], *candidates: str) -> Optional[str]:
    """
    Ø¨Ø§ ØªÙˆØ¬Ù‡ Ø¨Ù‡ Ù„ÛŒØ³Øª Ú©Ø§Ù†Ø¯ÛŒØ¯Ù‡Ø§ (canonical)ØŒ Ø§ÙˆÙ„ÛŒÙ† Ù…ÙˆØ±Ø¯ÛŒ Ú©Ù‡ Ø¯Ø± map Ù‡Ø³Øª Ø±Ø§ Ø¨Ø±Ù…ÛŒâ€ŒÚ¯Ø±Ø¯Ø§Ù†Ø¯.
    """
    for c in candidates:
        key = _canon(c)
        if key in header_map:
            return header_map[key]
    return None


def main() -> None:
    # =========================
    # 1) Read configs
    # =========================
    print("== READ CONFIGS ==")
    bot = read_config_bot()
    site = read_config_site()
    print("config_bot keys:", list(bot.keys())[:1])
    print("config_site keys:", list(site.keys())[:1])

    # =========================
    # 2) Read products (first 3)
    # =========================
    print("\n== READ PRODUCTS (first 3) ==")
    products = read_products()
    for p in products[:3]:
        _p(p)

    # Ø§Ú¯Ø± Ù…Ø­ØµÙˆÙ„ÛŒ Ù†Ø¨ÙˆØ¯ØŒ Ø§Ø¯Ø§Ù…Ù‡ Ù†Ø¯Ù‡ÛŒÙ…
    if not products:
        print("no products found; aborting smoketest.")
        return

    # =========================
    # 3) Read orders headers & make a robust row
    # =========================
    ws = _open_ws("orders")
    headers = ws.row_values(1)
    header_map = _build_header_map(headers)

    # Ù…Ø¹Ø§Ø¯Ù„â€ŒÙ‡Ø§ÛŒ Ú©ÙŽÙ†ÙÙ†ÛŒÚ©Ø§Ù„ Ú©Ù‡ Ù…ÛŒâ€ŒØ®ÙˆØ§Ù‡ÛŒÙ… Ù¾ÙØ± Ú©Ù†ÛŒÙ…:
    col_order_no = _pick(header_map, "Ø´Ù…Ø§Ø±Ù‡ Ø³ÙØ§Ø±Ø´", "order no", "order number", "order_no", "order id")
    col_type = _pick(header_map, "Ù†ÙˆØ¹", "Ù†ÙˆØ¹ Ø®Ø±Ø¯Ù‡/Ø¹Ù…Ø¯Ù‡", "type", "channel")
    col_customer_code = _pick(header_map, "Ú©Ø¯ Ù…Ø´ØªØ±ÛŒ", "customer code")
    col_telegram_id = _pick(header_map, "ØªÙ„Ú¯Ø±Ø§Ù… id", "telegram id", "telegram_id")
    col_receiver = _pick(header_map, "Ù†Ø§Ù… Ú¯ÛŒØ±Ù†Ø¯Ù‡", "Ù†Ø§Ù… Ú¯ÙŠØ±Ù†Ø¯Ù‡", "receiver name", "customer_name")
    col_mobile = _pick(header_map, "Ù…ÙˆØ¨Ø§ÛŒÙ„", "Ù…ÙˆØ¨Ø§ÙŠÙ„", "phone", "mobile", "Ø´Ù…Ø§Ø±Ù‡ ØªÙ…Ø§Ø³")
    col_address = _pick(header_map, "Ø¢Ø¯Ø±Ø³", "Ø§Ø¯Ø±Ø³", "address")
    col_postal = _pick(header_map, "Ú©Ø¯Ù¾Ø³ØªÛŒ", "Ú©Ø¯ Ù¾Ø³ØªÛŒ", "postal code", "postcode")
    col_items = _pick(header_map, "Ø§Ù‚Ù„Ø§Ù…", "Ø§Ù‚Ù„Ø§Ù… json", "items", "items json", "items_json")
    col_total = _pick(header_map, "Ø¬Ù…Ø¹ Ú©Ù„", "total", "amount", "final total")
    col_payment_method = _pick(header_map, "Ø±ÙˆØ´ Ù¾Ø±Ø¯Ø§Ø®Øª", "payment method")
    col_status = _pick(header_map, "ÙˆØ¶Ø¹ÛŒØª", "status", "ÙˆØ¶Ø¹ÛŒØª Ø³ÙØ§Ø±Ø´")
    col_created_at = _pick(header_map, "ØªØ§Ø±ÛŒØ® Ø«Ø¨Øª", "created at", "created_at", "date")
    col_receipt_url = _pick(header_map, "Ø±Ø³ÛŒØ¯ url", "Ø±Ø³ÛŒØ¯", "receipt url", "receipt_url")
    col_note = _pick(header_map, "ÛŒØ§Ø¯Ø¯Ø§Ø´Øª", "note", "notes", "ØªÙˆØ¶ÛŒØ­Ø§Øª")

    # Ø­Ø¯Ø§Ù‚Ù„ Ù„Ø§Ø²Ù… Ø¯Ø§Ø±ÛŒÙ…: Ø´Ù…Ø§Ø±Ù‡ Ø³ÙØ§Ø±Ø´ + ÙˆØ¶Ø¹ÛŒØª (Ø¨Ø±Ø§ÛŒ Ø¢Ù¾Ø¯ÛŒØª Ø¨Ø¹Ø¯ÛŒ)
    if not col_order_no or not col_status:
        print("orders sheet headers are missing required columns (order_no/status). aborting.")
        return

    print("\n== APPEND TEST ORDER ==")
    now = int(time.time())
    order_no = f"T{now}"

    # Ø¢Ù…Ø§Ø¯Ù‡â€ŒØ³Ø§Ø²ÛŒ Ø§Ù‚Ù„Ø§Ù… ØªØ³Øª Ø§Ø² Ø¯Ùˆ Ù…Ø­ØµÙˆÙ„ Ø§ÙˆÙ„
    items_payload = [
        {
            "code": (p.get("code") or p.get("Ú©Ø¯") or p.get("Ù†Ø§Ù…") or "P-TEST"),
            "qty": random.randint(1, 3),
        }
        for p in products[:2]
    ]
    items_json = json.dumps(items_payload, ensure_ascii=False)

    # Ø¯ÛŒÚ©Ø´Ù†Ø±ÛŒ Ø¨Ø§ Ú©Ù„ÛŒØ¯Ù‡Ø§ÛŒ ÙˆØ§Ù‚Ø¹ÛŒ Ù‡Ø¯Ø±Ù‡Ø§ (Ù‡Ø± Ú©Ø¯ÙˆÙ… Ø§Ú¯Ø± ÛŒØ§ÙØª Ù†Ø´Ø¯ØŒ Ø³Øª Ù†Ù…ÛŒâ€ŒØ´ÙˆØ¯)
    row: Dict[str, str] = {}
    row[col_order_no] = order_no
    if col_type:
        row[col_type] = "Ø®Ø±Ø¯Ù‡"
    if col_customer_code:
        row[col_customer_code] = "C-TEST"
    if col_telegram_id:
        row[col_telegram_id] = "123456789"
    if col_receiver:
        row[col_receiver] = "Ø³ÙØ§Ø±Ø´ ØªØ³Øª"
    if col_mobile:
        row[col_mobile] = "9120000000"
    if col_address:
        row[col_address] = "ØªÙ‡Ø±Ø§Ù†"
    if col_postal:
        row[col_postal] = "1111111111"
    if col_items:
        row[col_items] = items_json
    if col_total:
        row[col_total] = "1500000"
    if col_payment_method:
        row[col_payment_method] = ""
    if col_status:
        row[col_status] = "PENDING_PAYMENT"
    if col_created_at:
        row[col_created_at] = time.strftime("%Y-%m-%d %H:%M:%S")
    if col_receipt_url:
        row[col_receipt_url] = ""
    if col_note:
        row[col_note] = "Ø³ÙØ§Ø±Ø´ ØªØ³Øª Ø§Ø³Ù…ÙˆÚ©"

    appended_row = append_order(row)
    print(f"appended at row: {appended_row}")

    # =========================
    # 4) Update the same order status
    # =========================
    print("\n== UPDATE ORDER STATUS ==")
    ok = update_order_status(order_no, "PAID", "Ø±Ø³ÛŒØ¯ ØªØ§ÛŒÛŒØ¯ Ø´Ø¯")
    print("update status ok?", ok)


if __name__ == "__main__":
    main()
