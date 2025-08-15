import requests
import csv
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# ===================== تنظیمات =====================
CHAIN_ID = "solana"
TOKEN_LIMIT = 5000
MAX_THREADS = 8
BATCH_SIZE = 250
BATCH_PAUSE_SEC = 5

REQUEST_TIMEOUT = 30
RETRIES = 3
BACKOFF = 1.5
PER_REQUEST_SLEEP = 0.2

MIN_LIQUIDITY = 5000
MIN_VOLUME_24H = 10000
CANDLE_COUNT = 50  # تعداد کندل‌های بررسی برای مثلث و نهنگ‌ها
FOUR_HOUR_INTERVAL = 16  # 4 ساعت = 16 کندل 15 دقیقه‌ای

CSV_FILE = "pairs_liquidity.csv"
TIME_FMT = "%Y-%m-%d %H:%M:%S"

# ================= کمک‌توابع عمومی =================
def now_str():
    return datetime.utcnow().strftime(TIME_FMT)

def to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except:
        return default

def build_pair_url(pair: dict) -> str:
    if isinstance(pair.get("url"), str) and pair["url"].startswith("http"):
        return pair["url"]
    chain = pair.get("chainId") or CHAIN_ID
    addr = pair.get("pairAddress") or pair.get("pairId")
    if chain and addr:
        return f"https://dexscreener.com/{chain}/{addr}"
    return ""

# ================= گرفتن لیست توکن‌های سولانا =================
def fetch_solana_tokens(limit=TOKEN_LIMIT):
    url = "https://raw.githubusercontent.com/solana-labs/token-list/main/src/tokens/solana.tokenlist.json"
    for attempt in range(1, RETRIES+1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                tokens = data.get("tokens", [])
                tokens_with_website = [t for t in tokens if t.get('extensions', {}).get('website')]
                def popularity_score(token):
                    score = len(token.get('tags', []))
                    if token.get('extensions', {}).get('website'):
                        score += 3
                    if token.get('logoURI'):
                        score += 2
                    return score
                sorted_tokens = sorted(tokens_with_website, key=popularity_score, reverse=True)
                symbols = [t.get('symbol', '').strip() for t in sorted_tokens if t.get('symbol')]
                symbols = [s for s in symbols if 1 <= len(s) <= 20]
                return symbols[:limit]
            else:
                time.sleep(BACKOFF ** attempt)
        except Exception:
            time.sleep(BACKOFF ** attempt)
    return []

# ================= گرفتن جفت‌های یک نماد از Dexscreener =================
def fetch_pairs_for_symbol(symbol: str):
    url = f"https://api.dexscreener.com/latest/dex/search?q={symbol}&chainIds={CHAIN_ID}"
    last_exc = None
    for attempt in range(1, RETRIES+1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                pairs = data.get("pairs", []) or []
                filtered = []
                sym_u = (symbol or "").upper()
                for p in pairs:
                    base = (p.get("baseToken", {}) or {}).get("symbol", "")
                    quote = (p.get("quoteToken", {}) or {}).get("symbol", "")
                    if sym_u not in (base.upper(), quote.upper()):
                        continue
                    liq = to_float((p.get("liquidity", {}) or {}).get("usd"), 0)
                    vol24 = to_float((p.get("volume", {}) or {}).get("h24"), 0)
                    if liq >= MIN_LIQUIDITY and vol24 >= MIN_VOLUME_24H:
                        filtered.append(p)
                time.sleep(PER_REQUEST_SLEEP + random.uniform(0, 0.3))
                return filtered
            else:
                time.sleep(BACKOFF ** attempt)
        except Exception as e:
            last_exc = e
            time.sleep(BACKOFF ** attempt)
    return []

# ================= خواندن CSV قبلی =================
def load_last_liquidity_map(csv_file: str):
    if not os.path.exists(csv_file):
        return {}
    last_map = {}
    try:
        with open(csv_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return {}
            for row in reader:
                base_quote = row.get("Pair", "")
                if "/" not in base_quote:
                    continue
                base, quote = [x.strip() for x in base_quote.split("/", 1)]
                key = tuple(sorted([base.upper(), quote.upper()]))
                new_liq = to_float(row.get("Liquidity New"), 0.0)
                new_vol = to_float(row.get("Volume 24h New"), 0.0)
                last_map[key] = {"liq": new_liq, "vol": new_vol}
    except Exception:
        return {}
    return last_map

# ================= پردازش یک نماد =================
def process_symbol(symbol: str):
    pairs = fetch_pairs_for_symbol(symbol)
    best = {}
    for p in pairs:
        base = (p.get("baseToken", {}) or {}).get("symbol", "")
        quote = (p.get("quoteToken", {}) or {}).get("symbol", "")
        if not base or not quote:
            continue
        key = tuple(sorted([base.upper(), quote.upper()]))
        liq = to_float((p.get("liquidity", {}) or {}).get("usd"), 0)
        if key not in best or liq > to_float((best[key].get("liquidity", {}) or {}).get("usd"), 0):
            best[key] = p
    return best

# ================= بررسی الگوی مثلث صعودی =================
def check_ascending_triangle(candles):
    if len(candles) < 5:
        return False
    last_candles = candles[-CANDLE_COUNT:]
    highs = [to_float(c.get("high")) for c in last_candles]
    lows = [to_float(c.get("low")) for c in last_candles]
    max_high, min_high = max(highs), min(highs)
    if max_high == 0:
        return False
    if (max_high - min_high) / max_high > 0.01:
        return False
    for i in range(1, len(lows)):
        if lows[i] <= lows[i-1]:
            return False
    return True

# ================= بررسی فعالیت نهنگ =================
def check_whale_activity(candles):
    last_candles = candles[-CANDLE_COUNT:]
    buyer_vol = sum([to_float(c.get("buyVolume")) for c in last_candles])
    seller_vol = sum([to_float(c.get("sellVolume")) for c in last_candles])
    buyer_count = sum([to_float(c.get("buyCount")) for c in last_candles])
    seller_count = sum([to_float(c.get("sellCount")) for c in last_candles])
    if seller_count == 0:
        return False
    if (buyer_count / seller_count < 0.8) and (buyer_vol > seller_vol):
        return True
    return False

# ================= بررسی دایورجنس صعودی =================
def check_divergence(candles):
    if len(candles) < FOUR_HOUR_INTERVAL:
        return False
    last_candle = candles[-1]
    prev_candle = candles[-FOUR_HOUR_INTERVAL]
    price_new = to_float(last_candle.get("close"))
    price_old = to_float(prev_candle.get("close"))
    buy_vol_new = to_float(last_candle.get("buyVolume"))
    buy_vol_old = to_float(prev_candle.get("buyVolume"))
    if price_new < price_old and buy_vol_new >= 1.1 * buy_vol_old:
        return True
    return False

# ================= گرفتن کندل‌ها =================
def fetch_candles(pair_id):
    url = f"https://api.dexscreener.com/latest/dex/pairs/{CHAIN_ID}/{pair_id}/history?interval=15m"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            candles = data.get("candles", [])
            return candles
    except:
        pass
    return []

# ================= اجرای چندبخشی + پیشرفت =================
def fetch_all_best_pairs(token_symbols):
    all_best = {}
    n = len(token_symbols)
    last_percent = -1
    for start in range(0, n, BATCH_SIZE):
        end = min(start + BATCH_SIZE, n)
        batch = token_symbols[start:end]
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
            futures = {ex.submit(process_symbol, sym): sym for sym in batch}
            for i, fut in enumerate(as_completed(futures)):
                sym = futures[fut]
                try:
                    best_pairs = fut.result()
                    all_best.update(best_pairs)
                except:
                    continue
                percent = int((i / len(batch)) * 100)
                if percent != last_percent:
                    last_percent = percent
                    print(f"Batch {start}-{end} Progress: {percent}%")
        time.sleep(BATCH_PAUSE_SEC)
    return all_best

# ================= ذخیره CSV نهایی =================
def save_csv(pairs_map, last_map):
    fieldnames = [
        "Pair", "Liquidity New", "Liquidity Old", "Volume 24h New", "Volume 24h Old",
        "Ascending Triangle", "Whale Activity", "Divergence Up"
    ]
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for key, pair in pairs_map.items():
            base, quote = key
            liq_new = to_float((pair.get("liquidity", {}) or {}).get("usd"))
            vol_new = to_float((pair.get("volume", {}) or {}).get("h24"))
            old = last_map.get(key, {})
            liq_old = old.get("liq", 0.0)
            vol_old = old.get("vol", 0.0)
            pair_id = pair.get("pairAddress") or pair.get("pairId")
            candles = fetch_candles(pair_id)
            row = {
                "Pair": f"{base}/{quote}",
                "Liquidity New": liq_new,
                "Liquidity Old": liq_old,
                "Volume 24h New": vol_new,
                "Volume 24h Old": vol_old,
                "Ascending Triangle": check_ascending_triangle(candles),
                "Whale Activity": check_whale_activity(candles),
                "Divergence Up": check_divergence(candles)
            }
            writer.writerow(row)
    print(f"{now_str()} => CSV Saved with {len(pairs_map)} pairs")

# ================= اجرای اصلی =================
def main():
    print(f"{now_str()} => Fetching SOL tokens...")
    tokens = fetch_solana_tokens(limit=TOKEN_LIMIT)
    print(f"{now_str()} => {len(tokens)} tokens fetched. Processing pairs...")
    last_map = load_last_liquidity_map(CSV_FILE)
    all_best_pairs = fetch_all_best_pairs(tokens)
    save_csv(all_best_pairs, last_map)
    print(f"{now_str()} => Done.")

if __name__ == "__main__":
    main()
