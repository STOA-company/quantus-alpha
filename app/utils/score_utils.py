import pandas as pd
import numpy as np
from app.cache.factors import factors_cache


def calculate_factor_score(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    columns = df.columns.tolist()

    non_numeric_columns = ["Code", "Name", "ExchMnem", "WI26업종명(대)"]
    for col in columns:
        if col in non_numeric_columns:
            continue
        if not pd.api.types.is_numeric_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].fillna(df_copy[col].median())

    total_ranks = np.zeros(len(df_copy))
    descriptions = [[] for _ in range(len(df_copy))]

    for col in columns:
        config = factors_cache.get_configs().get(col)
        if not config:
            continue

        series = df_copy[col]
        ascending = config.get("direction") == "ASC"
        factor_range = config.get("range")

        if factor_range and factor_range != (None, None):
            min_val, max_val = factor_range
            ranks = series.rank(method="average", ascending=ascending)

            if min_val is not None:
                ranks[series < min_val] = len(df_copy)
            if max_val is not None:
                ranks[series > max_val] = len(df_copy)
        else:
            ranks = series.rank(method="average", ascending=ascending)

        total_ranks += ranks

        for i in range(len(df_copy)):
            value = series.iloc[i]
            rank = ranks.iloc[i]

            if factor_range and factor_range != (None, None):
                min_val, max_val = factor_range
                if min_val is not None and value < min_val:
                    description = f"범위 미만 (최소: {min_val})"
                elif max_val is not None and value > max_val:
                    description = f"범위 초과 (최대: {max_val})"
                else:
                    description = "낮을수록 좋음" if ascending else "높을수록 좋음"
            else:
                description = "낮을수록 좋음" if ascending else "높을수록 좋음"

            descriptions[i].append(f"{col}: {value} (순위: {int(rank)}위, {description})")

    score_df = pd.DataFrame({"Code": df["Code"], "score": np.zeros(len(df))})

    if np.any(total_ranks > 0):
        min_rank = total_ranks.min()
        max_rank = total_ranks.max()

        if min_rank != max_rank:
            scores = 100 * (1 - (total_ranks - min_rank) / (max_rank - min_rank))
            score_df["score"] = np.round(scores, 2)

    score_df["description"] = [" | ".join(description) for description in descriptions]

    return score_df.sort_values("score", ascending=False)
