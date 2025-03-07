import pandas as pd
import numpy as np
from app.cache.factors import factors_cache
from app.utils.test_utils import time_it
from app.common.constants import NON_NUMERIC_COLUMNS


@time_it
def calculate_factor_score_with_description(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    columns = df.columns.tolist()

    numeric_columns = [col for col in columns if col not in NON_NUMERIC_COLUMNS]

    for col in numeric_columns:
        df_copy[col] = df_copy[col].fillna(df_copy[col].median())

    n_rows = len(df_copy)

    factor_ranks = np.ones((n_rows, 0))
    max_ranks_per_factor = []  # 각 팩터의 최대 순위(꼴등) 저장
    descriptions = np.empty((n_rows, len(numeric_columns)), dtype=object)

    for col_idx, col in enumerate(numeric_columns):
        config = factors_cache.get_configs().get(col)
        if not config:
            continue

        series = df_copy[col]
        ascending = config.get("direction") == "ASC"

        ranks = series.rank(method="min", ascending=ascending)
        max_ranks_per_factor.append(ranks.max())  # 해당 팩터의 최대 순위(꼴등) 저장

        factor_ranks = np.column_stack((factor_ranks, ranks.values))

        for i in range(n_rows):
            value = series.iloc[i]
            rank = ranks.iloc[i]

            rank_str = "N/A" if pd.isna(rank) else f"{int(rank)}위"

            description = "낮을수록 좋음" if ascending else "높을수록 좋음"

            value_str = "N/A" if pd.isna(value) else str(value)
            descriptions[i, col_idx] = f"{col}: {value_str} (순위: {rank_str}, {description})"

    score_df = pd.DataFrame({"Code": df["Code"].values, "score": np.zeros(n_rows)})

    # 점수 계산 로직 (새로운 방식)
    if factor_ranks.shape[1] > 0:
        # 각 종목별로 각 팩터에서의 순위를 정규화 (1: 최고 순위, 0: 최저 순위)
        normalized_ranks = np.zeros_like(factor_ranks, dtype=float)

        for i, max_rank in enumerate(max_ranks_per_factor):
            if max_rank > 1:  # 순위가 여러 개인 경우만 정규화
                # (최대 순위 - 현재 순위) / (최대 순위 - 1)
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
    if df.empty:
        return pd.DataFrame()

    df_copy = df.copy()
    columns = df.columns.tolist()

    numeric_columns = [col for col in columns if col not in NON_NUMERIC_COLUMNS]

    # NaN -> 중앙값
    for col in numeric_columns:
        df_copy[col] = df_copy[col].fillna(df_copy[col].median())

    n_rows = len(df_copy)

    factor_ranks = np.ones((n_rows, 0))  # 각 팩터 별 순위 초기화
    max_ranks_per_factor = []  # 각 팩터의 최대 순위(꼴등) 저장

    for col in numeric_columns:
        config = factors_cache.get_configs().get(col)
        if not config:
            continue

        series = df_copy[col]
        ascending = config.get("direction") == "ASC"

        ranks = series.rank(method="min", ascending=ascending)
        max_ranks_per_factor.append(ranks.max())  # 해당 팩터의 최대 순위(꼴등) 저장

        factor_ranks = np.column_stack((factor_ranks, ranks.values))

    score_df = pd.DataFrame({"Code": df["Code"].values, "score": np.zeros(n_rows)})

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
