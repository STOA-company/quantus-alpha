#!/usr/bin/env python3
"""
이메일 전송 테스트
실제로 이메일을 보내서 기능을 확인합니다.
"""

import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.utils.email_utils import (
    send_email,
    create_notification_email,
    insert_string_into_html
)

def test_simple_email():
    """간단한 HTML 이메일 전송 테스트"""
    print("📧 간단한 HTML 이메일 전송 테스트")
    
    try:
        result = send_email(
            template="<h1>테스트 이메일</h1><p>이것은 테스트 이메일입니다.</p>",
            email="kknaks@stoa-investment.com",  # 실제 이메일 주소로 변경하세요
            subject="테스트 이메일 - HTML"
        )
        
        if result:
            print("✅ 이메일 전송 성공!")
        else:
            print("❌ 이메일 전송 실패")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def test_notification_template():
    """notification.html 템플릿 사용 테스트"""
    print("\n📧 notification.html 템플릿 테스트")
    
    try:
        # notification.html 템플릿 사용
        template = create_notification_email(
            greeting="안녕하세요!",
            content="이것은 notification.html 템플릿을 사용한 테스트 이메일입니다.",
            closing="감사합니다."
        )
        
        result = send_email(
            template=template,
            email="kknaks@stoa-investment.com",  # 실제 이메일 주소로 변경하세요
            subject="테스트 이메일 - notification 템플릿"
        )
        
        if result:
            print("✅ notification 템플릿 이메일 전송 성공!")
        else:
            print("❌ 이메일 전송 실패")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def test_email_with_attachment():
    """첨부 파일이 있는 이메일 전송 테스트"""
    print("\n📧 첨부 파일 이메일 전송 테스트")
    
    # 테스트용 텍스트 파일 생성
    test_file = "test_attachment.txt"
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write("이것은 테스트용 첨부 파일입니다.\n")
        f.write("이메일 첨부 기능을 테스트하기 위한 파일입니다.")
    
    try:
        result = send_email(
            template="<h1>첨부 파일 테스트</h1><p>첨부된 파일을 확인해주세요.</p>",
            email="kknaks@stoa-investment.com",  # 실제 이메일 주소로 변경하세요
            subject="테스트 이메일 - 첨부 파일",
            attachment_paths=test_file
        )
        
        if result:
            print("✅ 첨부 파일 이메일 전송 성공!")
        else:
            print("❌ 이메일 전송 실패")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        # 테스트 파일 삭제
        if os.path.exists(test_file):
            os.remove(test_file)

def test_manual_template():
    """수동으로 템플릿 처리하는 테스트"""
    print("\n📧 수동 템플릿 처리 테스트")
    
    try:
        # notification.html 파일 읽기
        template_path = "static/templates/email/notification.html"
        with open(template_path, 'r', encoding='utf-8') as f:
            template = f.read()
        
        # 플레이스홀더 치환
        template = insert_string_into_html(template, "{{ greeting }}", "안녕하세요!")
        template = insert_string_into_html(template, "{{ content }}", "수동으로 템플릿을 처리한 테스트 이메일입니다.")
        template = insert_string_into_html(template, "{{ closing }}", "감사합니다.")
        
        result = send_email(
            template=template,
            email="kknaks@stoa-investment.com",  # 실제 이메일 주소로 변경하세요
            subject="테스트 이메일 - 수동 템플릿"
        )
        
        if result:
            print("✅ 수동 템플릿 이메일 전송 성공!")
        else:
            print("❌ 이메일 전송 실패")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == '__main__':
    print("🚀 이메일 전송 테스트를 시작합니다...")
    print("=" * 50)
    
    # 실제 이메일 주소로 변경하세요
    print("⚠️  주의: 테스트를 실행하기 전에 'your-email@example.com'을 실제 이메일 주소로 변경하세요!")
    print("=" * 50)
    
    # 사용자 확인
    response = input("테스트를 실행하시겠습니까? (y/n): ")
    if response.lower() != 'y':
        print("테스트를 취소했습니다.")
        sys.exit(0)
    
    # 테스트 실행
    test_simple_email()
    test_notification_template()
    test_email_with_attachment()
    test_manual_template()
    
    print("\n" + "=" * 50)
    print("🎉 모든 테스트가 완료되었습니다!")
    print("받은 편지함을 확인해보세요.")
