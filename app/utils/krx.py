import json
import os
from datetime import datetime

import pandas as pd
import requests

from app.common.constants import KRX_DIR

# Request URL
url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

# Request Headers
headers = {
    "holdings": {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC020103010901",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    },
    "prices": {
        # 'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    },
    "base_information": {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC020103010901",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    },
}


def get_etf_holdings(isuCd: str, market_date=datetime.now().strftime("%Y%m%d")):
    bld = {
        "etf": "MDCSTAT05001",
    }

    data = {
        "bld": f'dbms/MDC/STAT/standard/{bld["etf"]}',
        "locale": "ko_KR",
        "mktId": "ALL",
        "trdDd": market_date,
        "isuCd": isuCd,
        "share": "1",
        "money": "1",
        "csvxls_isNo": False,
    }

    r = requests.post(url, data=data, headers=headers["holdings"])

    jo = json.loads(r.text)

    df = pd.DataFrame(jo["output"])

    return df


def get_all_prices(asset: str = "etf", market_date=datetime.now().strftime("%Y%m%d")):
    # 한국거래소(KRX): 모든 종목 기본 정보(종가) 크롤링 함수

    bld = {
        "stock": "MDCSTAT01501",
        "etf": "MDCSTAT04301",
        "etn": "MDCSTAT06401",
        "elw": "MDCSTAT06401",
    }

    if asset not in bld.keys():
        raise ValueError("올바른 상품을 입력하세요. (주식: stock, ETF: etf)")

    # Form Data
    # params = {
    #     'bld': f'dbms/MDC/STAT/standard/{bld[asset]}',
    #     'locale': 'ko_KR',
    #     'mktId': 'ALL',
    #     'trdDd': market_date,
    #     'share': '1',
    #     'money': '1',
    #     'csvxls_isNo': False
    # }
    # r = requests.get(url, params=params, headers=headers)

    data = {
        "bld": f"dbms/MDC/STAT/standard/{bld[asset]}",
        "locale": "ko_KR",
        "mktId": "ALL",
        "trdDd": market_date,
        "share": "1",
        "money": "1",
        "csvxls_isNo": False,
    }
    r = requests.post(url, data=data, headers=headers["prices"])

    jo = json.loads(r.text)

    if asset == "stock":
        df = pd.DataFrame(jo["OutBlock_1"])
    else:
        df = pd.DataFrame(jo["output"])

    return df


def get_kr_etf_base_information(is_download: bool = False):
    bld = {
        "etf": "MDCSTAT04601",
    }

    data = {"bld": f'dbms/MDC/STAT/standard/{bld["etf"]}', "locale": "ko_KR", "share": "1", "csvxls_isNo": False}

    r = requests.post(url, data=data, headers=headers["base_information"])

    jo = json.loads(r.text)

    df = pd.DataFrame(jo["output"])

    if is_download:
        df.to_parquet(os.path.join(KRX_DIR, "data_base.parquet"), index=False)

    return df


if __name__ == "__main__":
    df = get_all_prices(asset="etf")
