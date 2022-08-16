DOHLCV_COLS = ["DateTime", "Open", "High", "Low", "Close", "Volume"]
AGG_FREQS = ["15Min", "30Min", "1H", "4H", "1D"]
MA_PERIODS = [8, 13, 21, 34, 50, 200]
DERIVED_COLS = [
    ("Close Price >= VWAP", "Close", "VWAP"),
    ("Close Price >= 8 EMA", "Close", "8 EMA"),
    ("Close Price >= 13 EMA", "Close", "13 EMA"),
    ("Close Price >= 21 EMA", "Close", "21 EMA"),
    ("Close Price >= 34 EMA", "Close", "34 EMA"),
    ("Close Price >= 50 EMA", "Close", "50 EMA"),
    ("Close Price >= 200 EMA", "Close", "200 EMA"),
    ("21 EMA >= 34 EMA", "21 EMA", "34 EMA"),
    ("21 EMA >= 50 EMA", "21 EMA", "50 EMA"),
    ("21 EMA >= 200 EMA", "21 EMA", "200 EMA"),
]

AGG_MARKET_TICKERS = [
    "VXX",
    "SPY",
    "USO",
    "JNK",
    "IYR",
    "HYG",
    # Corp. Bonds
    "EEM",
    "GLD",
    "TLT",
]

MARKET_OPEN = "09:30:00"
MARKET_CLOSE = "16:00:00"

DATA_FOLDER = "./data"
OUTPUT_FOLDER = "./output"
