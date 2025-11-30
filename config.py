# config.py  
# ---------------------------------------------------------  
# Глобальная конфигурация арбитражного ядра  
# ---------------------------------------------------------

import sys
from pathlib import Path
from loguru import logger

# Создаём директорию для логов, если не существует
Path("logs").mkdir(exist_ok=True)

# ============================================================  
# СПИСОК БИРЖ, КОТОРЫЕ ИСПОЛЬЗУЕМ  
# Можно временно отключать биржи, которые нестабильны / блокируются  
# ============================================================

EXCHANGES = [  
    "bitget",  
    "gate",  
    "okx",  
    "bybit",
    "mexc",      # MEXC активен
    "bingx",     # BingX USDT-M фьючерсы
    "htx",       # HTX (ex-Huobi) USDT-M фьючерсы
]

# ============================================================  
# НАСТРОЙКИ БАЗЫ ДАННЫХ  
# ============================================================

DB_NAME = "bot_database.sqlite"

# ============================================================  
# WebSocket URL для фьючерсных рынков (стаканы)  
# ============================================================

WSS_URLS = {  
    # BYBIT linear futures v5  
    "bybit": "wss://stream.bybit.com/v5/public/linear",

    # BITGET USDT-M фьючерсы  
    "bitget": "wss://ws.bitget.com/v2/ws/public",

    # GATE — корректный WebSocket сервер для USDT-фьючерсов (USDT-фьючерсы FX)  
    "gate": "wss://fx-ws.gateio.ws/v4/ws/usdt",

    # OKX v5 public  
    "okx": "wss://ws.okx.com:8443/ws/v5/public",
    
    # MEXC USDT perpetual futures
    "mexc": "wss://contract.mexc.com/edge",
    
    # BINGX USDT-M perpetual futures (swap market)
    "bingx": "wss://open-api-swap.bingx.com/swap-market",
    
    # HTX (ex-Huobi) USDT-M linear perpetual futures
    "htx": "wss://api.hbdm.com/linear-swap-ws",
}

# ============================================================  
# КОМИССИИ (в процентах)  
# ============================================================

# Комиссия по умолчанию (если биржи нет в TAKER_FEES)  
# 0.06% = 0.0006 (в долях), но здесь значения храним как "0.06" именно в процентах.  
DEFAULT_TAKER_FEE = 0.06

# Per-exchange комиссии (для более точных расчётов), тоже в процентах.  
TAKER_FEES = {  
    "bybit": 0.055,  
    "bitget": 0.06,  
    "gate": 0.075,  
    "okx": 0.06,
    "mexc": 0.02,
    "bingx": 0.05,   # BingX taker fee 0.05%
    "htx": 0.05,     # HTX taker fee 0.05%
}

# ============================================================  
# ПАРАМЕТРЫ РИСКА / СТАКАНОВ  
# ============================================================

# Если стакан старше DEAD_WS_TIMEOUT — он считается устаревшим  
DEAD_WS_TIMEOUT = 5  # сек

# Множитель для "протухания" WS по отсутствию сообщений (используется в WsManager)  
WS_PING_INTERVAL = 15                 # FIX 5.2: пинг WS каждые 15 сек (было 20)  
WS_STALE_TIMEOUT_MULTIPLIER = 3       # фактически ~60 сек без сообщений

# Глобальные лимиты по риску (на будущее / валидация с фронта)  
MAX_ACTIVE_PAIRS = 30           # максимум одновременно активных пар (фронт тоже ограничивает до 30)  
MAX_PAIR_VOLUME = 10_000.0      # максимум объём по одной паре (в USDT, оценочно)  
MAX_TOTAL_RISK_USDT = 50_000.0  # общий лимит риска по всем парам

# Лимиты ядра (используются в main.py)  
# - MAX_OPEN_PAIRS: сколько пар одновременно могут иметь ОТКРЫТУЮ позицию  
# - MAX_MONITORED_PAIRS: сколько пар ядро реально обрабатывает за один тик  
MAX_OPEN_PAIRS = 5  
MAX_MONITORED_PAIRS = 30

# Биржевой рынок (swap/futures/spot)
DEFAULT_MARKET_TYPE = "swap"

# Плечо по умолчанию (задел на будущее)
DEFAULT_LEVERAGE = 10

# ============================================================
# РЕЖИМ ПОЗИЦИИ НА БИРЖАХ (hedge / one_way)
# ============================================================
# hedge - можно одновременно иметь LONG и SHORT позиции
# one_way - только одна позиция (net position)
#
# ВАЖНО: Убедитесь что режим на бирже соответствует настройке здесь!
# Для арбитража рекомендуется Hedge mode на всех биржах.

POSITION_MODES = {
    # ВАЖНО: Режим должен соответствовать настройке на аккаунте биржи!
    # one_way = односторонняя позиция (net position) - более универсальный
    # hedge = двусторонняя позиция (можно LONG и SHORT одновременно)
    "bitget": "one_way",   # One-way mode (unilateral position)
    "bingx": "one_way",    # One-way mode
    "binance": "one_way",
    "bybit": "one_way",
    "okx": "one_way",      # One-way mode (net position)
    "gate": "one_way",
    "mexc": "one_way",
    "htx": "one_way",
}

# ============================================================  
# ЛОГИРОВАНИЕ  
# ============================================================

logger.remove()

# Основной лог в консоль  
logger.add(  
    sys.stderr,  
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",  
    level="INFO",  
)

# Полный DEBUG лог  
logger.add(  
    "logs/debug.log",  
    rotation="10 MB",  
    level="DEBUG",  
    compression="zip",  
    retention="2 days",  
)

# Лог торговых операций (TRADE=True)  
logger.add(  
    "logs/trades.log",  
    rotation="5 MB",  
    level="INFO",  
    filter=lambda record: "TRADE" in record["extra"],  
)

logger.info("⚙ Конфигурация арбитражного ядра загружена.")

# ============================================================  
# ПРОЧИЕ ИНТЕРВАЛЫ  
# ============================================================
PRICE_UPDATE_INTERVAL = 1 # секунды между тиками основного цикла


# ============================================================
# ПОРОГИ ДИСБАЛАНСА
# ============================================================

WARNING_IMBALANCE_PCT = 5.0    # предупреждение при дисбалансе > 5%
CRITICAL_IMBALANCE_PCT = 10.0  # критический дисбаланс > 10%
