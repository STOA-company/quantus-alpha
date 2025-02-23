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
    rank_list = []

    for col in columns:
        config = factors_cache.get_configs().get(col)
        if not config:
            continue

        series = df_copy[col]
        ascending = config.get("direction") == "ASC"
        ranks = series.rank(method="average", ascending=ascending)
        total_ranks += ranks

        value = series.iloc[0]
        rank = ranks.iloc[0]
        direction_str = "낮을수록 좋음" if ascending else "높을수록 좋음"

        rank_list.append(f"{col}: {value:.2f} (순위: {rank}위, {direction_str})")

    score_df = pd.DataFrame({"Code": df["Code"], "score": np.zeros(len(df)), "rank_list": ""})

    if np.any(total_ranks > 0):
        min_rank = total_ranks.min()
        max_rank = total_ranks.max()

        if min_rank != max_rank:
            scores = 100 * (1 - (total_ranks - min_rank) / (max_rank - min_rank))
            score_df["score"] = np.round(scores, 2)

    score_df["rank_list"] = " | ".join(rank_list)

    return score_df.sort_values("score", ascending=False)
