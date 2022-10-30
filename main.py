import pandas as pd
from datetime import datetime
from stock_indicators.indicators.common import Quote
from stock_indicators import indicators
from typing import List, Tuple
from constants import (
    DOHLCV_COLS,
    AGG_FREQS,
    DERIVED_COLS,
    MA_PERIODS,
    DATA_FOLDER,
    OUTPUT_FOLDER,
    MARKET_OPEN,
    MARKET_CLOSE,
    AGG_MARKET_TICKERS,
)
from os import listdir
import os
import sys
from os.path import isfile, join
import itertools
import threading
from pathlib import Path
import time
import sys
from tqdm import tqdm
from utils import (
    get_EMAs,
    read_data,
    derive_timeframes_from_5_min_data,
    add_EMA_derived_cols,
    add_technicals,
    add_price_action,
    get_agg_market_data,
    get_quotes_tuple_from_df,
)


def main():
    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)
    get_agg_market_data()
    for f in listdir(DATA_FOLDER):
        if isfile(join(DATA_FOLDER, f)):
            ticker = f.split("_")[0]
            if Path(f"./{OUTPUT_FOLDER}/{ticker}.csv").is_file():
                continue
            (market_data_5m, quote_list_5m) = read_data(f)
            df_timeframes = derive_timeframes_from_5_min_data(market_data_5m)
            consolidated_data = pd.DataFrame(columns=DOHLCV_COLS)
            for i, (market_data, quote_list) in enumerate(
                [
                    (market_data_5m, quote_list_5m),
                    *df_timeframes,
                ]
            ):
                current_timeframe = ["5Min", *AGG_FREQS][i]

                print(f"\nAnalyzing \033[1;32m{current_timeframe}\033[0m data")
                df = add_EMA_derived_cols(
                    (get_EMAs((market_data, quote_list)), quote_list)
                )
                df = add_technicals((df, quote_list))
                df = add_price_action((df, quote_list))
                df["Time Frame"] = current_timeframe
                consolidated_data = pd.concat([consolidated_data, df])
            consolidated_data.to_csv(  # .merge(agg_market_data, on="DateTime")
                f"./{OUTPUT_FOLDER}/{ticker}.csv"
            )


if __name__ == "__main__":
    main()
    # try:
    #     t = threading.Thread(target=animate)
    #     t.start()
    #     done = True
    # except KeyboardInterrupt:
    #     done = True


# done = False


# def animate():
#     bar = [
#         " [==     ]",
#         " [ ==    ]",
#         " [  ==   ]",
#         " [   ==  ]",
#         " [    == ]",
#         " [     ==]",
#         " [    == ]",
#         " [   ==  ]",
#         " [  ==   ]",
#         " [ ==    ]",
#     ]
#     # ["|", "/", "-", "\\"]
#     for c in itertools.cycle(bar):
#         if done:
#             break
#         sys.stdout.write(c + "\r")
#         sys.stdout.flush()
#         time.sleep(0.1)
#     sys.stdout.write("\rDone!     ")


# def main():
# bar = [
#     " [=     ]",
#     " [ =    ]",
#     " [  =   ]",
#     " [   =  ]",
#     " [    = ]",
#     " [     =]",
#     " [    = ]",
#     " [   =  ]",
#     " [  =   ]",
#     " [ =    ]",
# ]
# i = 0

# while main1():
#     print(bar[i % len(bar)], end="\r")
#     time.sleep(0.2)
#     i += 1
