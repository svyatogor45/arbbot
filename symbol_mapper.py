# symbol_mapper.py  
# ---------------------------------------------------  
# Унифицированный маппер символов для всех модулей:  
# WS, CCXT, логика MarketEngine и TradeEngine.  
# ---------------------------------------------------

from typing import Tuple

# Поддерживаемые QUOTE для автоматического парсинга  
QUOTE_CANDIDATES = ["USDT", "USDC", "BUSD"]

# ==============================  
# БАЗОВАЯ НОРМАЛИЗАЦИЯ  
# ==============================

def normalize_base_quote(symbol: str) -> Tuple[str, str]:  
    """  
    Возвращает (BASE, QUOTE) в верхнем регистре  
    из множества возможных форматов:  
      BTC/USDT  
      BTC-USDT  
      BTC_USDT  
      BTCUSDT  
      BTC-USDT-SWAP  
      BTCUSDT_UMCBL  
      BTCUSDT-UMCBL  
      BTC/USDT:USDT  
    """  
    s = symbol.strip().upper()

    # 1. CCXT unified: BTC/USDT:USDT → берём только левую часть до ':'  
    if ":" in s:  
        s = s.split(":", 1)[0]

    # 2. Стрип суффиксов (OKX, Bitget, другие)  
    SUFFIXES = [  
        "-SWAP", "_UMCBL", "-UMCBL", "_CMCBL", "-CMCBL",  
        "-PERP", "_PERP", "-FUTURES"  
    ]  
    for suf in SUFFIXES:  
        if s.endswith(suf):  
            s = s[: -len(suf)]  
            break

    # 3. Разделители  
    for sep in ("/", "-", "_"):  
        if sep in s:  
            base, quote = s.split(sep, 1)  
            return base, quote

    # 4. Попытка выделить QUOTE по известным суффиксам  
    for q in QUOTE_CANDIDATES:  
        if s.endswith(q):  
            base = s[:-len(q)]  
            if base:  
                return base, q

    # 5. fallback  
    return s, ""

# ==============================  
# INTERNAL SYMBOL  
# ==============================

def to_internal(symbol: str) -> str:  
    """Приводит символ к виду BASEQUOTE (например BTCUSDT)."""  
    base, quote = normalize_base_quote(symbol)  
    return f"{base}{quote}"

def from_internal(symbol: str) -> str:  
    """INTERNAL → человекочитаемый BASE/QUOTE."""  
    s = symbol.upper()  
    for q in QUOTE_CANDIDATES:  
        if s.endswith(q):  
            base = s[:-len(q)]  
            return f"{base}/{q}"  
    return s

# ==============================  
# WS SYMBOLS PER EXCHANGE  
# ==============================

def to_ws_symbol(exchange: str, symbol: str) -> str:  
    """Символ для WS-подписок под требования конкретной биржи."""  
    exchange = exchange.lower()  
    base, quote = normalize_base_quote(symbol)  
    internal = f"{base}{quote}"

    # BYBIT  
    if exchange == "bybit":  
        return internal

    # BITGET futures  
    if exchange == "bitget":  
        return internal

    # GATE USDT futures  
    if exchange == "gate":  
        return f"{base}_{quote}"

    # OKX futures swap  
    if exchange == "okx":  
        return f"{base}-{quote}-SWAP"

    # MEXC USDT perpetual futures  
    if exchange == "mexc":  
        return f"{base}_{quote}"

    # BINGX USDT-M perpetual futures (формат: BTC-USDT)
    if exchange == "bingx":  
        return f"{base}-{quote}"

    # HTX (ex-Huobi) USDT-M perpetual futures (формат: BTC-USDT)
    if exchange == "htx":  
        return f"{base}-{quote}"

    return internal

# ==============================  
# CCXT SYMBOLS (UNIFIED)  
# ==============================

def to_ccxt_symbol(exchange: str, symbol: str) -> str:  
    """Преобразование в unified CCXT формат BASE/QUOTE:QUOTE."""  
    base, quote = normalize_base_quote(symbol)  
    return f"{base}/{quote}:{quote}"

# ==============================  
# PRETTY SYMBOL FOR LOGS/UI  
# ==============================

def pretty(symbol: str) -> str:  
    """Красивое отображение BASE/QUOTE."""  
    base, quote = normalize_base_quote(symbol)  
    if quote:  
        return f"{base}/{quote}"  
    return symbol.upper()

