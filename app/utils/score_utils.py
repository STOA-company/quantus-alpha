import pandas as pd
import numpy as np
from app.cache.factors import factors_cache, etf_factors_cache
from app.utils.test_utils import time_it


class ScoreUtils:
    def __init__(self, asset_type: str):
        self.factors_cache = etf_factors_cache if asset_type == "etf" else factors_cache
        self.asset_type = asset_type

    @time_it
    def calculate_factor_score(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df_copy = df.copy()
        columns = df_copy.columns.tolist()

        # 데이터 타입을 직접 확인하여 숫자형 컬럼 식별
        numeric_columns = [col for col in columns if pd.api.types.is_numeric_dtype(df_copy[col])]

        if df_copy.empty:
            return pd.DataFrame()

        n_rows = len(df_copy)

        factor_ranks = np.ones((n_rows, 0))  # 각 팩터 별 순위 초기화
        max_ranks_per_factor = []

        for col in numeric_columns:
            df_copy = df_copy[~df_copy[col].isna()]

            config = self.factors_cache.get_configs().get(col)
            if not config:
                continue

            ascending = config.get("direction") == "ASC"
            min_value = config.get("min_value")
            max_value = config.get("max_value")

            outlier_mask = pd.Series(False, index=df_copy.index)
            if min_value is not None:
                outlier_mask = df_copy[col] < min_value
            if max_value is not None:
                outlier_mask = df_copy[col] > max_value

            if outlier_mask.any():
                if ascending:
                    df_copy.loc[outlier_mask, col] = float("inf")
                else:
                    df_copy.loc[outlier_mask, col] = float("-inf")

            ranks = df_copy[col].rank(method="min", ascending=ascending)
            max_ranks_per_factor.append(ranks.max())  # 해당 팩터의 최대 순위(꼴등) 저장

            factor_ranks = np.column_stack((factor_ranks, ranks.values))

        score_df = pd.DataFrame({"Code": df_copy["Code"].values, "score": np.zeros(n_rows)})

        if factor_ranks.shape[1] > 0:
            # 종목 별 순위를 정규화 (1: 최고 순위, 0: 최저 순위)
            normalized_ranks = np.zeros_like(factor_ranks, dtype=float)

            for i, max_rank in enumerate(max_ranks_per_factor):
                if max_rank > 1:  # 순위가 여러 개인 경우만 정규화
                    # (최대 순위 - 현재(해당 종목) 순위) / (최대 순위 - 1)
                    normalized_ranks[:, i] = (max_rank - factor_ranks[:, i]) / (max_rank - 1)
                else:
                    normalized_ranks[:, i] = 1.0  # 모든 종목이 동일한 순위일 경우

            # 정규화된 순위의 평균 (0~1 사이 값)
            avg_normalized_ranks = np.mean(normalized_ranks, axis=1)

            # 0~1 범위를 0~100 점수로 변환
            scores = 100 * avg_normalized_ranks

            # 모든 팩터에서 정확히 1등인 경우에만 100점 부여
            all_first = np.all(factor_ranks == 1, axis=1)

            # 모든 팩터에서 꼴등인 경우
            all_last = np.ones(n_rows, dtype=bool)
            for i, max_rank in enumerate(max_ranks_per_factor):
                all_last &= factor_ranks[:, i] == max_rank

            # 모든 팩터에서 1등이 아닌 종목은 최대 99.99점
            scores[~all_first] = np.minimum(scores[~all_first], 99.99)

            # 모든 팩터에서 꼴등인 종목은 0점
            scores[all_last] = 0.0

            score_df["score"] = np.round(scores, 2)

        return score_df.sort_values("score", ascending=False)


score_utils = ScoreUtils(asset_type="stock")
etf_score_utils = ScoreUtils(asset_type="etf")
