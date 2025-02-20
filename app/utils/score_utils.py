import pandas as pd
import numpy as np
from app.common.constants import FACTOR_CONFIGS


def calculate_factor_score(df: pd.DataFrame) -> pd.DataFrame:
    """팩터 점수 계산 후 code, score만 반환"""
    df_copy = df.copy()
    columns = df.columns.tolist()
    # NaN -> 중앙값
    non_numeric_columns = ["Code", "name", "market", "sector"]
    for col in columns:
        if col in non_numeric_columns:
            continue
        if not pd.api.types.is_numeric_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].fillna(df_copy[col].median())

    total_ranks = np.zeros(len(df_copy))

    for col in columns:
        config = FACTOR_CONFIGS.get(col)
        if not config:
            print(f"Warning: No configuration found for column {col}")
            continue

        series = df_copy[col]

        if config.get("range"):
            min_range, max_range = config["range"]

            ranks = np.full(len(series), len(series))

            if min_range is not None:
                outliers = series < min_range
                if outliers.any():
                    ranks[outliers] = len(series)

            if max_range is not None:
                outliers = series > max_range
                if outliers.any():
                    ranks[outliers] = len(series)

            total_ranks += ranks
            continue

        if "optimal_range" in config:
            min_opt, max_opt = config["optimal_range"]
            ranks = np.full(len(series), len(series))
            mask = (series >= min_opt) & (series <= max_opt)
            ranks[mask] = 1
            ranks[~mask] = np.abs(series[~mask] - ((min_opt + max_opt) / 2)).rank(method="average") + mask.sum()
            total_ranks += ranks
            continue

        if "optimal_value" in config:
            optimal = config["optimal_value"]
            ranks = np.abs(series - optimal).rank(method="average")
            total_ranks += ranks
            continue

        ascending = config.get("direction", 1) == 1
        ranks = series.rank(method="average", ascending=ascending)
        total_ranks += ranks

    score_df = pd.DataFrame({"Code": df["Code"], "score": np.zeros(len(df))})

    if np.any(total_ranks > 0):
        min_rank = total_ranks.min()
        max_rank = total_ranks.max()

        if min_rank != max_rank:
            # 0 ~ 100
            scores = 100 * (1 - (total_ranks - min_rank) / (max_rank - min_rank))
            score_df["score"] = np.round(scores, 2)

    return score_df.sort_values("score", ascending=False)
