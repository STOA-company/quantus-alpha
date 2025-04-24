import json
import os
from asyncio.log import logger
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
    "detail_information": {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC020103010901",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
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

    df.to_parquet(os.path.join(KRX_DIR, f"{asset}_prices.parquet"), index=False)

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


def get_kr_etf_detail_information():
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT05101",
        "locale": "ko_KR",
        "idxMktClssId2": "",
        "inqCondTpCd1": "0",
        "inqCondTpCd3": "0",
        "inqCondTpCd4": "0",
        "inqCondTpCd2": "0",
        "srchStrNm": "",
        "idxAsstClssId1": "00",
        "idxMktClssId": "00",
        "idxMktClssId3": "01",
        "idxMktClssId1": "02",
        "countryBox2": ["0201", "0202", "0203", "0204", "0205", "0206", "0207", "0208"],
        "countryBox1": "",
        "idxAsstClssId2": "00",
        "idxAsstClssId3": "00",
        "taxTpCd": "0",
        "idxLvrgInvrsTpCd": "TT",
        "asstcomId": "00000",
        "gubun": "1",
        "trdDd": "20250423",
        "strtDd": "20250321",
        "endDd": "20250423",
        "inqCondTp1_Box1": "0",
        "inqCondTp2_Box1": "0",
        "inqCondTp3_Box1": "0",
        "inqCondTp4_Box1": "0",
        "inqCondTpCd5": "0",
        "inqCondTp1_Box2": "0",
        "inqCondTp3_Box2": "0",
        "inqCondTp4_Box2": "0",
        "inqCondTpCd6": "1",
        "sortMethdTpCd": "2",
        "inqCondTp2_Box2": "0",
        "inqCondTpCd7": "0",
        "inqCondTpCd8": "0",
        "inqCondTpCd9": "0",
        "money": "3",
        "csvxls_isNo": False,
    }

    r = requests.post(url, data=data, headers=headers["detail_information"])

    jo = json.loads(r.text)

    df = pd.DataFrame(jo["output"])

    df.to_parquet(os.path.join(KRX_DIR, "data_detail.parquet"), index=False)

    return df


def create_etf_integrated_info():
    """ETF 통합 정보 생성 (시가총액, 운용사, 상장일, 운용자산, NAV, 발행주식수 등)"""
    try:
        # 기본 정보 가져오기
        base_df = get_kr_etf_base_information()

        # 상세 정보 가져오기
        detail_df = get_kr_etf_detail_information()

        # 가격 정보 가져오기
        price_df = get_all_prices(asset="etf")

        # 필요한 컬럼들만 선택하여 통합
        integrated_df = pd.DataFrame()

        # 기본 정보에서 필요한 컬럼 추가 (종목코드, 종목명, 상장일, 운용사)
        if not base_df.empty:
            integrated_df = base_df[["ISU_CD", "ISU_NM", "LIST_DD", "COM_ABBRV"]]
            integrated_df.columns = ["표준코드", "한글종목명", "상장일", "운용사"]

        # 가격 정보에서 필요한 컬럼 추가 (NAV, 시가총액, 상장좌수)
        if not price_df.empty:
            price_cols = {
                "ISU_CD": "표준코드",
                "NAV": "순자산가치(NAV)",
                "MKTCAP": "시가총액",
                "LIST_SHRS": "상장좌수",
            }
            price_selected = price_df[list(price_cols.keys())].rename(columns=price_cols)
            integrated_df = pd.merge(integrated_df, price_selected, on="표준코드", how="left")

        # 상세 정보에서 순자산총액 추가
        if not detail_df.empty:
            detail_cols = {"ISU_CD": "표준코드", "NETASST_TOTAMT": "순자산총액"}
            detail_selected = detail_df[list(detail_cols.keys())].rename(columns=detail_cols)
            integrated_df = pd.merge(integrated_df, detail_selected, on="표준코드", how="left")

        # 데이터 타입 변환
        numeric_columns = ["순자산가치(NAV)", "시가총액", "상장좌수", "순자산총액"]
        for col in numeric_columns:
            if col in integrated_df.columns:
                integrated_df[col] = pd.to_numeric(integrated_df[col].str.replace(",", ""), errors="coerce")

        # 결과 저장
        output_path = os.path.join(KRX_DIR, "etf_integrated.parquet")
        integrated_df.to_parquet(output_path, index=False)

        return integrated_df

    except Exception as e:
        logger.error(f"ETF 통합 정보 생성 중 오류 발생: {str(e)}")
        raise


if __name__ == "__main__":
    create_etf_integrated_info()
