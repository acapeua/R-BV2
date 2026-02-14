import os
import time
import requests
import ccxt
import pandas as pd
from dotenv import load_dotenv
from ta.momentum import RSIIndicator

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

RSI_LOW = float(os.getenv("RSI_LOW", 30))
RSI_HIGH = float(os.getenv("RSI_HIGH", 70))

TIMEFRAMES = [x.strip() for x in os.getenv("TIMEFRAMES", "5m,15m,1h").split(",")]
SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))

# prioridade fixa
EXCHANGES = ["bybit", "kucoin", "okx"]

CCXT_TIMEOUT_MS = int(os.getenv("CCXT_TIMEOUT_MS", "15000"))
ENABLE_RATE_LIMIT = os.getenv("ENABLE_RATE_LIMIT", "1") == "1"

# pin por (symbol, tf)
pinned_exchange = {}  # (SYMBOL, TF) -> exchange_name

# cooldown por exchange quando falhar (evita ficar tentando a que está ruim)
cooldown_until = {ex: 0 for ex in EXCHANGES}
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "120"))

# estado RSI por (exchange|market_symbol|tf)
last_state = {}

# cache de market_symbol perp encontrado por (exchange, base_symbol)
perp_symbol_cache = {}  # (exchange, "BTC/USDT") -> "BTC/USDT:USDT" etc.


def send(msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=15)
    except Exception as e:
        print("Erro ao enviar Telegram:", e)


def build_exchange(name: str):
    klass = getattr(ccxt, name)
    ex = klass({
        "enableRateLimit": ENABLE_RATE_LIMIT,
        "timeout": CCXT_TIMEOUT_MS,
        "options": {"defaultType": "swap"},
    })
    return ex


exchanges = {name: build_exchange(name) for name in EXCHANGES}


def is_recoverable(e: Exception) -> bool:
    recoverable_types = (
        ccxt.NetworkError,
        ccxt.ExchangeNotAvailable,
        ccxt.DDoSProtection,
        ccxt.RateLimitExceeded,
        ccxt.RequestTimeout,
    )
    if isinstance(e, recoverable_types):
        return True
    msg = str(e).lower()
    if "restricted" in msg or "forbidden" in msg or "403" in msg:
        return True
    return False


def find_perp_market_symbol(ex, base_symbol: str) -> str:
    """
    Mapeia 'BTC/USDT' para o símbolo perp/swap real da exchange.
    Preferência: swap linear (USDT-margined).
    Usa cache pra não chamar load_markets toda hora.
    """
    cache_key = (ex.id, base_symbol)
    if cache_key in perp_symbol_cache:
        return perp_symbol_cache[cache_key]

    ex.load_markets()

    base, quote = base_symbol.split("/")
    candidates = []

    for m in ex.markets.values():
        if not m.get("active", True):
            continue
        if m.get("base") != base or m.get("quote") != quote:
            continue
        if m.get("swap") is True or m.get("type") == "swap":
            candidates.append(m)

    if not candidates:
        # fallback: se existir o símbolo base diretamente, tenta
        if base_symbol in ex.symbols:
            perp_symbol_cache[cache_key] = base_symbol
            return base_symbol
        raise ccxt.BadSymbol(f"Perp {base_symbol} não encontrado em {ex.id}")

    linear = [m for m in candidates if m.get("linear") is True]
    chosen = (linear[0] if linear else candidates[0])["symbol"]

    perp_symbol_cache[cache_key] = chosen
    return chosen


def exchange_order_for(symbol: str, tf: str):
    """
    Retorna a ordem de tentativa respeitando o PIN.
    Se houver pin, tenta primeiro a pinned, depois as demais na ordem de prioridade.
    Aplica cooldown.
    """
    now = time.time()
    pin = pinned_exchange.get((symbol, tf))

    order = []
    if pin in EXCHANGES:
        order.append(pin)

    for ex in EXCHANGES:
        if ex not in order:
            order.append(ex)

    # filtra por cooldown
    order = [ex for ex in order if now >= cooldown_until.get(ex, 0)]
    return order


def fetch_ohlcv_pinned(base_symbol: str, timeframe: str, limit: int = 100):
    """
    Busca OHLCV respeitando o PIN (exchange travada por symbol+tf).
    Se a pinned falhar, tenta as outras; se uma funcionar, atualiza o PIN.
    """
    last_err = None
    tried = []

    for ex_name in exchange_order_for(base_symbol, timeframe):
        tried.append(ex_name)
        ex = exchanges[ex_name]

        try:
            market_symbol = find_perp_market_symbol(ex, base_symbol)
            candles = ex.fetch_ohlcv(market_symbol, timeframe, limit=limit)

            # se funcionou, fixa pin
            prev_pin = pinned_exchange.get((base_symbol, timeframe))
            pinned_exchange[(base_symbol, timeframe)] = ex_name

            # se mudou a exchange, avisa (opcional, mas útil)
            if prev_pin and prev_pin != ex_name:
                send(f"🔁 Failover: {base_symbol} TF {timeframe} mudou de {prev_pin} → {ex_name}")

            return ex_name, market_symbol, candles

        except Exception as e:
            last_err = e

            # coloca a exchange em cooldown se erro “recuperável”/instabilidade
            if is_recoverable(e):
                cooldown_until[ex_name] = time.time() + COOLDOWN_SECONDS

            # símbolo não existe -> tenta próxima, mas sem cooldown
            if isinstance(e, ccxt.BadSymbol):
                continue

            # limpa cache do perp_symbol se o erro sugere mercado inválido (às vezes markets mudam)
            msg = str(e).lower()
            if "symbol" in msg or "market" in msg:
                perp_symbol_cache.pop((ex.id, base_symbol), None)

            continue

    raise RuntimeError(
        f"Nenhuma exchange respondeu para PERP {base_symbol} {timeframe}. "
        f"Tentadas: {tried}. Último erro: {last_err}"
    )


def get_rsi(base_symbol: str, tf: str):
    ex_name, market_symbol, candles = fetch_ohlcv_pinned(base_symbol, tf, limit=100)
    df = pd.DataFrame(candles, columns=["t", "o", "h", "l", "c", "v"])
    rsi = RSIIndicator(df["c"], window=14).rsi()
    return ex_name, market_symbol, float(rsi.iloc[-2]), float(rsi.iloc[-1])


def run():
    send("🤖 Bot RSI PERP multi-exchange (PINNED) iniciado.")
    send(f"📌 Base: {SYMBOL}\n⏱️ TFs: {', '.join(TIMEFRAMES)}\n🔁 Prioridade: bybit → kucoin → okx\n🧊 Cooldown: {COOLDOWN_SECONDS}s")

    while True:
        for tf in TIMEFRAMES:
            try:
                ex_name, market_symbol, prev_rsi, curr_rsi = get_rsi(SYMBOL, tf)
                key = f"{ex_name}|{market_symbol}|{tf}"

                if key not in last_state:
                    last_state[key] = curr_rsi
                    continue

                if prev_rsi > RSI_LOW and curr_rsi <= RSI_LOW:
                    send(
                        f"🔻 RSI CRUZOU ABAIXO\n"
                        f"{market_symbol}\nTF: {tf}\nRSI: {curr_rsi:.2f}\nEX: {ex_name}"
                    )

                if prev_rsi < RSI_HIGH and curr_rsi >= RSI_HIGH:
                    send(
                        f"🔺 RSI CRUZOU ACIMA\n"
                        f"{market_symbol}\nTF: {tf}\nRSI: {curr_rsi:.2f}\nEX: {ex_name}"
                    )

                last_state[key] = curr_rsi

            except Exception as e:
                print(f"Erro no TF {tf}:", e)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()

