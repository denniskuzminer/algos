import pandas as pd
from datetime import datetime, timedelta
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
from pathlib import Path
import itertools
import threading
import numpy as np
import time
import sys
from tqdm import tqdm


def get_EMAs(
    df_list: Tuple[pd.DataFrame, List[Quote]], periods=MA_PERIODS
) -> pd.DataFrame:
    (data_, quotes_list) = df_list
    for period in periods:
        print(f"Getting {period} EMA", end=", ", flush=True)
        result = indicators.get_ema(quotes_list, period)
        ma_df = pd.DataFrame.from_dict(
            {
                "DateTime": [r.date for r in result],
                f"{period} EMA": [r.ema for r in result],
            }
        )

        data_ = data_.merge(ma_df, how="inner", on="DateTime")

    return data_


def derive_timeframes_from_5_min_data(
    df: pd.DataFrame, freqs=AGG_FREQS
) -> List[Tuple[pd.DataFrame, List[Quote]]]:
    all_timeframes = []
    for freq in freqs:
        print("Deriving " + freq + " data", end="; ", flush=True)
        df_ = df.groupby(pd.Grouper(freq=freq)).agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        df_.columns = DOHLCV_COLS[1:]
        df_.dropna(subset=DOHLCV_COLS[1:-1], inplace=True)
        all_timeframes.append(get_quotes_tuple_from_df(df_))
    return all_timeframes


def getIntradayVWAP(row, quotes_list):
    result = indicators.get_vwap(
        [quotes_list[row.name]],
        start=datetime(
            row["DateTime"].year, row["DateTime"].month, row["DateTime"].day
        ),
    )
    row_num += 1
    return r[0].vwap


def add_EMA_derived_cols(df_list: Tuple[pd.DataFrame, List[Quote]]) -> pd.DataFrame:
    print("Getting EMA-derived columns")
    (computed_data, quotes_list) = df_list
    print("Getting Regular Market Hours", end=", ", flush=True)
    computed_data["Regular Market Hours"] = (
        computed_data["DateTime"].dt.time >= pd.to_datetime(MARKET_OPEN).time()
    ) & (computed_data["DateTime"].dt.time <= pd.to_datetime(MARKET_CLOSE).time())

    print("Market Pulse", end=", ", flush=True)

    computed_data["Market Pulse"] = (
        computed_data["8 EMA"] > computed_data["21 EMA"]
    ) & (computed_data["21 EMA"] > computed_data["34 EMA"])

    print("VWAP", end=", ", flush=True)
    # global row_num
    # row_num = 0
    computed_data["Intraday VWAP"] = computed_data.apply(
        lambda row: getIntradayVWAP(row, quotes_list), axis=1
    )

    # make it so that every day at 930 it will reset change it
    result = indicators.get_vwap(quotes_list, start=datetime(2021, 9, 1))
    vwap_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "VWAP": [r.vwap for r in result],
        }
    )
    computed_data = computed_data.merge(vwap_df, how="inner", on="DateTime")

    for (name, l, r) in DERIVED_COLS:
        print(name, end=", ", flush=True)

        computed_data[name] = pd.to_numeric(
            computed_data[l], errors="coerce"
        ) >= pd.to_numeric(computed_data[r], errors="coerce")

    return computed_data.infer_objects()


def add_technicals(df_list: Tuple[pd.DataFrame, List[Quote]]) -> pd.DataFrame:
    print("Getting technical indicators")
    (computed_data, quotes_list) = df_list
    # MACD
    print("Getting MACD", end=", ", flush=True)

    result = indicators.get_macd(quotes_list)
    macd_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "MACD": [r.macd for r in result],
            "MACD Fast EMA": [r.fast_ema for r in result],
            "MACD Slow EMA": [r.slow_ema for r in result],
        }
    )
    computed_data = computed_data.merge(macd_df, how="inner", on="DateTime")
    # CCI
    print("CCI", end=", ", flush=True)

    result = indicators.get_cci(quotes_list)
    cci_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "CCI": [r.cci for r in result],
        }
    )
    computed_data = computed_data.merge(cci_df, how="inner", on="DateTime")
    # ATR
    print("ATR", end=", ", flush=True)

    result = indicators.get_atr(quotes_list)
    atr_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "ATR": [r.atr for r in result],
        }
    )
    computed_data = computed_data.merge(atr_df, how="inner", on="DateTime")
    # ZScore
    print("ZScore", end=", ", flush=True)

    result = indicators.get_stdev(quotes_list, 120)
    stdev_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "ZScore": [r.stdev for r in result],
        }
    )
    computed_data = computed_data.merge(stdev_df, how="inner", on="DateTime")
    # Parabolic SAR
    print("Parabolic SAR", end=", ", flush=True)

    result = indicators.get_parabolic_sar(quotes_list)
    sar_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "Parabolic SAR": [r.sar for r in result],
        }
    )
    computed_data = computed_data.merge(sar_df, how="inner", on="DateTime")

    print("Parabolic SAR >= Close", end=", ", flush=True)

    computed_data["Parabolic SAR >= Close"] = (
        computed_data["Parabolic SAR"] >= computed_data["Close"]
    )
    print("Parabolic SAR Trend", end=", ", flush=True)

    computed_data["Parabolic SAR Trend"] = (
        computed_data["Parabolic SAR"] <= computed_data["Close"]
    )

    # Balance of power
    print("Balance of power", end=", ", flush=True)

    result = indicators.get_bop(quotes_list)
    sar_df = pd.DataFrame.from_dict(
        {
            "DateTime": [r.date for r in result],
            "Balance of Power": [r.bop for r in result],
        }
    )
    computed_data = computed_data.merge(sar_df, how="inner", on="DateTime")

    squeeze_types = {"Normal": 1.5, "Aggressive": 2, "Slow": 1}
    for squeeze_type in dict.keys(squeeze_types):
        print(squeeze_type + " TTM Squeeze", end=", ", flush=True)

        computed_data = TTM_Squeeze(
            computed_data,
            "Close",
            20,
            squeeze_types[squeeze_type],
            2.0,
            squeeze_type,
            1.0,
        )
        print(squeeze_type + " Squeeze Fired", end=", ", flush=True)
        computed_data[f"{squeeze_type} Squeeze Fired"] = (
            computed_data[f"{squeeze_type} TTM Squeeze On"].shift()
            & ~computed_data[f"{squeeze_type} TTM Squeeze On"]
        )

    return computed_data


def add_price_action(df_list: Tuple[pd.DataFrame, List[Quote]]) -> pd.DataFrame:
    print("Getting price action")

    (df, quotes_list) = df_list
    init_cols = df.columns
    df.columns = [*["open", "high", "low", "close", "vol"], *df.columns[5:]]
    df = candlestick.inverted_hammer(market_data, target="Is Inverted Hammer")
    df = candlestick.hammer(df, target="Is Hammer")
    df = candlestick.bearish_engulfing(df, target="Is Bearish Engulfing")
    df = candlestick.bullish_engulfing(df, target="Is Bullish Engulfing")
    df.columns = [
        *init_cols,
        "Is Inverted Hammer",
        "Is Hammer",
        "Is Bearish Engulfing",
        "Is Bullish Engulfing",
    ]
    return df


# TTM Squeeze - https://github.com/hackingthemarkets/ttm-squeeze
# https://tlc.thinkorswim.com/center/reference/Tech-Indicators/studies-library/T-U/TTM-Squeeze
def TTM_Squeeze(
    computed_data, price, length, nk, nbb, name, alert_line=1
) -> pd.DataFrame:
    avg = f"{length} SMA"
    computed_data[avg] = computed_data[price].rolling(window=length).mean()
    computed_data[f"{name} Lower Bollinger Band"] = pd.to_numeric(
        computed_data[avg], errors="coerce"
    ) - (nbb * pd.to_numeric(computed_data["ZScore"], errors="coerce"))
    computed_data[f"{name} Upper Bollinger Band"] = pd.to_numeric(
        computed_data[avg], errors="coerce"
    ) + (nbb * pd.to_numeric(computed_data["ZScore"], errors="coerce"))
    computed_data[f"{name} Lower Keltner Channel"] = pd.to_numeric(
        computed_data[avg], errors="coerce"
    ) - (pd.to_numeric(computed_data["ATR"], errors="coerce") * nk)
    computed_data[f"{name} Upper Keltner Channel"] = pd.to_numeric(
        computed_data[avg], errors="coerce"
    ) + (pd.to_numeric(computed_data["ATR"], errors="coerce") * nk)
    computed_data[f"{name} TTM Squeeze On"] = (
        computed_data[f"{name} Lower Bollinger Band"]
        > computed_data[f"{name} Lower Keltner Channel"]
    ) & (
        computed_data[f"{name} Upper Bollinger Band"]
        < computed_data[f"{name} Upper Keltner Channel"]
    )
    return computed_data


def read_data(file_name: str) -> Tuple[pd.DataFrame, List[Quote]]:
    print(
        "------------- Getting data from \033[1;32m\x1B[4m\033[1m"
        + file_name
        + "\033[0m\x1B[0m\033[0m -------------"
    )
    print("Approx: 5Min: 1:30; 15Min: 0:30; 30Min: 0:15; 1H: 0:09; 4H: 0:02; 1D: 0:00")
    market_data = pd.read_csv(
        f"{DATA_FOLDER}/{file_name}",
        header=None,
    )
    market_data = market_data.infer_objects()
    market_data.columns = DOHLCV_COLS
    market_data["DateTime"] = pd.to_datetime(market_data["DateTime"])
    market_data = market_data.set_index("DateTime")
    return get_quotes_tuple_from_df(market_data)  # (market_data, [])  #


def get_agg_market_data():
    market_path = f"{OUTPUT_FOLDER}/AGG_DATA.csv"
    f = Path(market_path)
    if f.is_file():
        return pd.read_csv(market_path)
    agg_data = pd.DataFrame()
    for ticker in tqdm(AGG_MARKET_TICKERS):
        (market_data_5m, quote_list_5m) = read_data(f"{ticker}_5min.txt")
        df_ = market_data_5m.groupby(pd.Grouper(freq="1D")).agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        df_.columns = DOHLCV_COLS[1:]
        df_.dropna(subset=DOHLCV_COLS[1:-1], inplace=True)
        (daily_data, daily_quotes) = get_quotes_tuple_from_df(df_)
        daily_data = get_EMAs((daily_data, daily_quotes), [8, 21, 34])
        daily_data["Daily Market Pulse"] = (
            daily_data["8 EMA"] > daily_data["21 EMA"]
        ) & (daily_data["21 EMA"] > daily_data["34 EMA"])
        # check this

        # print(market_data_5m.head())
        market_data_5m["Date"] = pd.to_datetime(
            market_data_5m.index.date
        )  # pd.to_datetime(market_data_5m.index).dt.date
        # print(pd.to_datetime(market_data_5m.head(50).index).date)
        # market_data_5m["Time"] =timedelta(hours=duration_obj.hour, minutes=duration_obj.minute

        # market_data_5m["Open Time"] = pd.to_datetime(
        #     market_data_5m["Date"].astype(str) + " " + "09:30:00"
        # )
        # market_data_5m["Date"] = pd.to_datetime(market_data_5m["Date"]).date

        pd.set_option("display.max_rows", 500)
        market_data_5m.reset_index(inplace=True)
        market_data_5m["First Time"] = (
            pd.to_datetime(market_data_5m["DateTime"]).dt.date
            != pd.to_datetime(market_data_5m["DateTime"].shift()).dt.date
        )
        # market_data_5m["Open Price"] = market_data_5m.loc[
        #     market_data_5m["Open Time"] == market_data_5m.index
        # ]["Open"]
        # market_data_5m["Open Price"].fillna(method="ffill", inplace=True)
        # print("")
        # print(market_data_5m.head(10))
        # print(daily_data.head(10))
        # print(market_data_5m.Date.dtype)
        # print(daily_data.DateTime.dtype)
        # market_data_5m_open = pd.DataFrame()
        # market_data_5m_open["DateTime"] = market_data_5m.index
        market_data_5m["Daily Open"] = np.where(
            market_data_5m["First Time"], market_data_5m["Open"], None
        )
        market_data_5m["Daily Open"] = market_data_5m["Daily Open"].ffill()
        # (
        #     market_data_5m["Open"]
        #     if market_data_5m["First Time"]
        #     else np.NaN
        # )
        market_data_5m["Ticker"] = ticker
        market_data_5m["Price >= Today's Open Price"] = (
            market_data_5m["Open"] >= market_data_5m["Daily Open"]
        )
        # print("")
        # print(
        #     market_data_5m.head(10)
        #     .reset_index()
        #     .rename(columns={market_data_5m.columns[0]: "DateTime 5min"})
        # )
        market_data_5m = (
            market_data_5m.reset_index()
            .rename(columns={market_data_5m.columns[0]: "DateTime 5min"})
            .merge(
                daily_data.reset_index(),
                how="outer",
                left_on="Date",
                right_on="DateTime",
            )
        )
        # print(market_data_5m.head(10).columns)
        agg_data = pd.concat(
            [
                agg_data,
                market_data_5m[
                    [
                        "DateTime 5min",
                        "Ticker",
                        "Price >= Today's Open Price",
                        "Daily Market Pulse",
                    ]
                ].rename(columns={"DateTime 5min": "DateTime"}),
            ]
        )

    agg_data.to_csv(market_path)
    return agg_data


def get_quotes_tuple_from_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Quote]]:
    print("Converting data types")
    li = []
    for d, o, h, l, c, v in tqdm(
        zip(
            df["DateTime"] if "DateTime" in df else df.index,
            df["Open"],
            df["High"],
            df["Low"],
            df["Close"],
            df["Volume"],
        )
    ):
        # print(f"{d} {o} {h} {l} {c} {v}", end="\r")
        try:
            li.append(Quote(d, o, h, l, c, v))
        except:
            # print((d, o, h, l, c, v))
            pass
    return (df, li)
    # return (
    #     df,
    #     [
    #         Quote(d, o, h, l, c, v)
    #         for d, o, h, l, c, v in tqdm(
    #             zip(
    #                 df["DateTime"] if "DateTime" in df else df.index,
    #                 df["Open"],
    #                 df["High"],
    #                 df["Low"],
    #                 df["Close"],
    #                 df["Volume"],
    #             )
    #         )
    #     ],
    # )
