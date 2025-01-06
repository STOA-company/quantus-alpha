import re


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
