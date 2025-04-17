import sys
import time

import requests


def test_streaming():
    # 서버 주소 - 필요시 변경
    url = "http://localhost/api/v1/chat/stream"
    params = {"query": "안녕하세요, 간단한 인사말 부탁드립니다", "model": "gpt4mi"}

    print(f"요청 URL: {url}")
    print(f"요청 파라미터: {params}")
    print("\n스트리밍 응답 시작:")
    print("-" * 80)

    start_time = time.time()

    try:
        # 디버깅을 위해 타임아웃 증가
        response = requests.get(url, params=params, stream=True, timeout=120)
        response.raise_for_status()  # HTTP 오류 검사

        print(f"응답 상태: {response.status_code}")
        print(f"응답 헤더: {dict(response.headers)}")

        # 청크를 바이트로 수신하고 16진수로 출력
        for i, chunk in enumerate(response.iter_content(chunk_size=10)):
            if chunk:
                hex_chunk = " ".join([f"{b:02x}" for b in chunk])
                print(f"청크 {i+1}: {hex_chunk} | 텍스트: {chunk.decode('utf-8', errors='replace')}")
                sys.stdout.flush()  # 강제로 출력 버퍼 비우기

            # 최대 20개 청크만 수신
            if i >= 20:
                print("최대 청크 수 도달, 종료...")
                break

        print("\n" + "-" * 80)
        print(f"총 소요 시간: {time.time() - start_time:.2f}초")

    except requests.exceptions.RequestException as e:
        print(f"\n오류 발생: {e}")
        print(f"총 소요 시간: {time.time() - start_time:.2f}초")

    print("\n테스트 완료")


if __name__ == "__main__":
    test_streaming()
