import pandas as pd
import numpy as np
from app.cache.factors import factors_cache
from app.utils.test_utils import time_it


@time_it
def calculate_factor_score_with_description(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    columns = df.columns.tolist()

    non_numeric_columns = ["Code", "Name", "country", "market", "sector"]
    numeric_columns = [col for col in columns if col not in non_numeric_columns]

    for col in numeric_columns:
        df_copy[col] = df_copy[col].fillna(df_copy[col].median())

    n_rows = len(df_copy)

    # 각 팩터별 순위를 저장할 배열
    factor_ranks = np.ones((n_rows, 0))
    descriptions = np.empty((n_rows, len(numeric_columns)), dtype=object)

    for col_idx, col in enumerate(numeric_columns):
        config = factors_cache.get_configs().get(col)
        if not config:
            continue

        series = df_copy[col]
        ascending = config.get("direction") == "ASC"
        factor_range = config.get("range")

        ranks = series.rank(method="min", ascending=ascending)

        if factor_range and factor_range != (None, None):
            min_val, max_val = factor_range
            if min_val is not None:
                ranks = ranks.mask(series < min_val, n_rows)
            if max_val is not None:
                ranks = ranks.mask(series > max_val, n_rows)

        # 팩터별 순위를 2D 배열에 추가
        factor_ranks = np.column_stack((factor_ranks, ranks.values))

        for i in range(n_rows):
            value = series.iloc[i]
            rank = ranks.iloc[i]

            rank_str = "N/A" if pd.isna(rank) else f"{int(rank)}위"

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

            value_str = "N/A" if pd.isna(value) else str(value)
            descriptions[i, col_idx] = f"{col}: {value_str} (순위: {rank_str}, {description})"

    score_df = pd.DataFrame({"Code": df["Code"].values, "score": np.zeros(n_rows)})

    # 점수 계산 로직 (새로운 방식)
    if factor_ranks.shape[1] > 0:
        # 각 종목의 모든 팩터 순위 평균
        avg_ranks = np.mean(factor_ranks, axis=1)

        # 최고 순위(1)와 최저 순위 간의 간격에 기반한 점수 계산
        max_rank = np.max(avg_ranks)
        if max_rank > 1:
            scores = 100 * (1 - (avg_ranks - 1) / (max_rank - 1))
        else:
            scores = np.ones(n_rows) * 100  # 모든 종목이 1등인 경우

        # 모든 팩터에서 정확히 1등인 경우에만 100점 부여
        all_first = np.all(factor_ranks == 1, axis=1)

        # 모든 팩터에서 1등이 아닌 종목은 최대 99.99점
        scores[~all_first] = np.minimum(scores[~all_first], 99.99)

        score_df["score"] = np.round(scores, 2)

    # 설명 문자열 결합
    joined_descriptions = []
    for i in range(n_rows):
        valid_descs = [d for d in descriptions[i] if d is not None and not pd.isna(d)]
        joined_descriptions.append(" | ".join(valid_descs))

    score_df["description"] = joined_descriptions

    # 정렬 및 반환
    return score_df.sort_values("score", ascending=False)


@time_it
def calculate_factor_score(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    columns = df.columns.tolist()

    # 비수치 컬럼 사전 필터링
    non_numeric_columns = ["Code", "Name", "country", "market", "sector"]
    numeric_columns = [col for col in columns if col not in non_numeric_columns]

    # 한 번에 모든 NaN 값을 중앙값으로 채움
    for col in numeric_columns:
        df_copy[col] = df_copy[col].fillna(df_copy[col].median())

    n_rows = len(df_copy)

    # 각 팩터 별 순위
    factor_ranks = np.ones((n_rows, 0))

    for col in numeric_columns:
        config = factors_cache.get_configs().get(col)
        if not config:
            continue

        series = df_copy[col]
        ascending = config.get("direction") == "ASC"
        factor_range = config.get("range")

        ranks = series.rank(method="min", ascending=ascending)

        if factor_range and factor_range != (None, None):
            min_val, max_val = factor_range
            if min_val is not None:
                ranks = ranks.mask(series < min_val, n_rows)
            if max_val is not None:
                ranks = ranks.mask(series > max_val, n_rows)

        factor_ranks = np.column_stack((factor_ranks, ranks.values))

    score_df = pd.DataFrame({"Code": df["Code"].values, "score": np.zeros(n_rows)})

    if factor_ranks.shape[1] > 0:
        avg_ranks = np.mean(factor_ranks, axis=1)

        # 최고 순위(1)와 최저 순위 간의 간격에 기반한 점수 계산
        max_rank = np.max(avg_ranks)
        if max_rank > 1:
            scores = 100 * (1 - (avg_ranks - 1) / (max_rank - 1))
        else:
            scores = np.ones(n_rows) * 100  # 모든 종목이 1등인 경우

        # 모든 팩터에서 정확히 1등인 경우에만 100점 부여
        all_first = np.all(factor_ranks == 1, axis=1)

        scores[~all_first] = np.minimum(scores[~all_first], 99.99)

        score_df["score"] = np.round(scores, 2)

    return score_df.sort_values("score", ascending=False)
