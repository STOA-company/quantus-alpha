
import os
import re
import logging
import asyncio
from typing import Literal, List, Union, Optional, Callable
from datetime import timedelta
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from app.common.email_configs import email_infos
from tenacity import retry, stop_after_attempt, wait_fixed


# 로거 설정
from app.core.logger import get_logger


logger = get_logger(__name__)

# 사용자 이메일 조회 함수 타입 정의
UserEmailProvider = Callable[[int], Optional[str]]

# 전역 사용자 이메일 제공자 (외부에서 설정 가능)
_user_email_provider: Optional[UserEmailProvider] = None

def set_user_email_provider(provider: UserEmailProvider) -> None:
    """사용자 이메일 조회 함수를 설정합니다.
    
    Args:
        provider: 사용자 ID를 받아서 이메일을 반환하는 함수
    """
    global _user_email_provider
    _user_email_provider = provider

def get_user_email(uid: int) -> Optional[str]:
    """사용자 ID로부터 이메일을 조회합니다.
    
    Args:
        uid: 사용자 ID
        
    Returns:
        Optional[str]: 사용자 이메일 주소 (없으면 None)
    """
    if _user_email_provider is None:
        logger.warning("사용자 이메일 제공자가 설정되지 않았습니다.")
        return None
    
    try:
        return _user_email_provider(uid)
    except Exception as e:
        logger.error(f"사용자 이메일 조회 중 오류 발생: {str(e)}", exc_info=True)
        return None

def insert_string_into_html(html_content, placeholder, string_to_insert):
    """placeholer를 이용하여 이메일 템플릿에 문자열 삽입

    Args:
        html_content (str): 업데이트 할 html content
        placeholder (str): placeholder
        string_to_insert (str): 삽입할 내용

    Returns:
        updated_html_content: 업데이트 된 html content
    """
    # 문자열을 삽입할 위치(플레이스홀더)를 찾습니다.
    placeholder_start = html_content.find(placeholder)

    # 플레이스홀더가 존재하지 않는 경우에는 그대로 반환합니다.
    if placeholder_start == -1:
        return html_content

    # 문자열을 삽입할 위치에 원하는 문자열을 삽입합니다.
    placeholder_end = placeholder_start + len(placeholder)
    updated_html_content = html_content[:placeholder_start] + string_to_insert + html_content[placeholder_end:]

    return updated_html_content

def create_notification_email(
    greeting: str,
    content: str, 
    closing: str,
    template_path: str = "static/templates/email/notification.html"
) -> str:
    """notification.html 템플릿을 사용하여 이메일 HTML을 생성합니다.
    
    Args:
        greeting: 인사말 ({{ greeting }} 플레이스홀더)
        content: 본문 내용 ({{ content }} 플레이스홀더)
        closing: 마무리 인사 ({{ closing }} 플레이스홀더)
        template_path: 템플릿 파일 경로
        
    Returns:
        str: 완성된 HTML 템플릿
    """
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template = f.read()
        
        # 플레이스홀더 치환
        template = insert_string_into_html(template, "{{ greeting }}", greeting)
        template = insert_string_into_html(template, "{{ content }}", content)
        template = insert_string_into_html(template, "{{ closing }}", closing)
        
        return template
    except Exception as e:
        logger.error(f"템플릿 파일 읽기 중 오류 발생: {str(e)}")
        raise

def validate_email(email: str) -> bool:
    """이메일 주소 유효성 검사
    
    Args:
        email (str): 검사할 이메일 주소
        
    Returns:
        bool: 유효한 이메일 주소인지 여부
    """
    if not email or not isinstance(email, str):
        return False
    
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, email))

@retry(stop=stop_after_attempt(3), wait=wait_fixed(60))
async def send_email(
    template: str, 
    email: str, 
    subject: str, 
    attachment_paths: Union[str, List[str]] = None,
    email_type: Literal["insight", "info", "adds1", "adds2", "adds3"] = "info",
) -> bool:
    """
    HTML 템플릿과 첨부 파일을 포함한 이메일을 전송합니다.
    
    Args:
        template: HTML 형식의 이메일 본문
        email: 수신자 이메일 주소
        subject: 이메일 제목
        attachment_paths: 첨부할 파일 경로 (단일 문자열 또는 경로 리스트)
        email_type: 이메일 발송 계정 유형
    
    Returns:
        bool: 이메일 발송 성공 여부
        
    Raises:
        ValueError: 이메일 주소가 유효하지 않은 경우
        Exception: 이메일 전송 중 오류 발생 시
    """
    # 이메일 유효성 검사
    if not validate_email(email):
        error_msg = f"유효하지 않은 이메일 주소: {email}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # 이메일 메시지를 생성합니다.
    email_info = email_infos[email_type]
    sender_email, password = email_info["email"], email_info["password"]

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = email
    message['Subject'] = subject

    # HTML 형식의 내용을 추가합니다.
    message.attach(MIMEText(template, 'html'))
    
    # 첨부 파일 처리
    logger.info(f"첨부파일 경로 받음: {attachment_paths}")
    if attachment_paths:
        # 단일 문자열이면 리스트로 변환
        if isinstance(attachment_paths, str):
            attachment_paths = [attachment_paths]
            logger.info(f"문자열을 리스트로 변환: {attachment_paths}")
            
        for file_path in attachment_paths:
            logger.info(f"처리할 파일 경로: {file_path}")
            if os.path.exists(file_path):
                filename = os.path.basename(file_path)
                logger.info(f"추출된 파일명: {filename}")
                
                # 한글 파일명 처리
                logger.info(f"원본 파일명: {filename}")
                
                file_extension = os.path.splitext(file_path)[1].lower()
                
                # 파일 타입에 따른 MIME 타입 설정
                mime_types = {
                    '.pdf': 'application/pdf',
                    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    '.xls': 'application/vnd.ms-excel',
                    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    '.doc': 'application/msword',
                    '.txt': 'text/plain',
                    '.csv': 'text/csv',
                    '.png': 'image/png',
                    '.jpg': 'image/jpeg',
                    '.jpeg': 'image/jpeg',
                    '.gif': 'image/gif',
                }
                
                mime_type = mime_types.get(file_extension, 'application/octet-stream')
                
                # 파일 첨부
                with open(file_path, 'rb') as attachment:
                    if mime_type.startswith('text/'):
                        # 텍스트 파일인 경우
                        part = MIMEText(attachment.read(), _subtype=mime_type.split('/')[1])
                    else:
                        # 바이너리 파일인 경우 (PDF, Excel 등)
                        part = MIMEBase(mime_type.split('/')[0], mime_type.split('/')[1])
                        part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                
                # 헤더 설정 - RFC 2231 표준으로 UTF-8 인코딩 명시
                part.add_header('Content-Type', mime_type)
                part.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', filename))
                
                logger.info(f"Content-Disposition 헤더 설정 완료: filename=('utf-8', '', '{filename}')")
                logger.info(f"Content-Type 헤더: {mime_type}")
                
                message.attach(part)
                logger.info(f"파일이 성공적으로 첨부되었습니다: {filename}")
            else:
                logger.warning(f"첨부 파일을 찾을 수 없습니다: {file_path}")

    try:
        # SMTP 서버에 연결합니다.
        smtp_server = smtplib.SMTP('smtp.gmail.com', 587, timeout=100)
        smtp_server.starttls()

        # 로그인합니다.
        smtp_server.login(sender_email, password)

        # 이메일을 전송합니다.
        smtp_server.sendmail(sender_email, email, message.as_string())

        smtp_server.quit()

        logger.info(f"이메일이 성공적으로 전송되었습니다. 수신자: {email}, 제목: {subject}")
        return True
    except Exception as e:
        error_msg = f"이메일 전송 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        # 오류를 다시 발생시켜 retry 메커니즘이 작동하도록 합니다
        raise