from enum import Enum


class FinancialSelect(str, Enum):
    REVENUE = "revenue"  # 매출 - 기본
    OPERATING_PROFIT = "operating_profit"  # 영업이익
    NET_INCOME = "net_income"  # 당기순이익
    EPS = "eps"  # 주당순이익(Earnings Per Share)
    
