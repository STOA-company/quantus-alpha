"""
앱 기능 테스트 - CI 파이프라인 확인용
"""

import os


def test_env_exists():
    """환경 변수 파일이 존재하는지 테스트"""
    assert os.path.exists(".env") or os.path.exists(".env.sample"), ".env 파일이 존재해야 합니다"


def test_dockerfile_exists():
    """Dockerfile이 존재하는지 테스트"""
    assert os.path.exists("Dockerfile"), "Dockerfile이 존재해야 합니다"


def test_docker_compose_exists():
    """docker-compose.yml 파일이 존재하는지 테스트"""
    assert os.path.exists("docker-compose.yml"), "docker-compose.yml 파일이 존재해야 합니다"
