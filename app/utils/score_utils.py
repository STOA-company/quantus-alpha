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
    factor_details = []

    for col in columns:
        config = factors_cache.get_configs().get(col)
        if not config:
            print(f"Warning: No configuration found for column {col}")
            continue

        series = df_copy[col]

        if config.get("range"):
            min_range, max_range = config["range"]
            ranks = np.full(len(series), len(series))

            outlier_info = []
            if min_range is not None:
                outliers = series < min_range
                if outliers.any():
                    ranks[outliers] = len(series)
                    outlier_info.append(f"< {min_range}")

            if max_range is not None:
                outliers = series > max_range
                if outliers.any():
                    ranks[outliers] = len(series)
                    outlier_info.append(f"> {max_range}")

            if outlier_info:
                factor_details.append(f"{col}: 이상치({', '.join(outlier_info)})")
            else:
                factor_details.append(f"{col}: 정상범위")

            total_ranks += ranks
            continue

        ascending = config.get("direction", 1) == 1
        ranks = series.rank(method="average", ascending=ascending)
        direction_str = "오름차순" if ascending else "내림차순"
        value = series.iloc[0]  # 해당 종목의 팩터 값
        rank_value = ranks.iloc[0]  # 해당 종목의 순위
        factor_details.append(f"{col}: {value:.2f} (순위: {rank_value:.0f}, {direction_str})")

        total_ranks += ranks

    score_df = pd.DataFrame(
        {
            "Code": df["Code"],
            "score": np.zeros(len(df)),
            "factor_analysis": "",
        }
    )

    if np.any(total_ranks > 0):
        min_rank = total_ranks.min()
        max_rank = total_ranks.max()

        if min_rank != max_rank:
            scores = 100 * (1 - (total_ranks - min_rank) / (max_rank - min_rank))
            score_df["score"] = np.round(scores, 2)

    score_df["factor_analysis"] = " | ".join(factor_details)

    return score_df.sort_values("score", ascending=False)
