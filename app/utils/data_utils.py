import re
import math
import numpy as np


def remove_parentheses(text):
    try:
        if not text:  # None이나 빈 문자열 체크
            return text
        # \(.*?\)$ : 마지막에 있는 괄호와 그 내용을 매칭
        # .*? : 괄호 안의 모든 문자 (non-greedy)
        cleaned_text = re.sub(r"\(.*?\)", "", text).strip()
        return cleaned_text
    except Exception as e:
        print(f"error {e}")
        return text


def ceil_to_integer(value):
    """
    소수점 첫째자리에서 올림을 수행하여 정수를 반환합니다.

    Args:
        value: 변환할 숫자 값

    Returns:
        int 또는 None: 변환된 정수 값 또는 None(입력이 None 또는 inf인 경우)
    """
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return None

    return int(math.ceil(float(value)))


def floor_to_integer(value):
    """
    소수점 첫째자리에서 내림을 수행하여 정수를 반환합니다.

    Args:
        value: 변환할 숫자 값

    Returns:
        int 또는 None: 변환된 정수 값 또는 None(입력이 None 또는 inf인 경우)
    """
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return None

    return int(math.floor(float(value)))
