"""
임시 테스트 파일 - CI 파이프라인 확인용
"""


def test_dummy_true():
    """항상 성공하는 더미 테스트"""
    assert True


def test_simple_addition():
    """간단한 덧셈 테스트"""
    assert 1 + 1 == 2


def test_string_concat():
    """문자열 연결 테스트"""
    assert "hello " + "world" == "hello world"
