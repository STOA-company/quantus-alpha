import json
import time
from typing import List, Optional, Dict, Any
from enum import Enum

from app.core.redis import redis_client
from app.core.logger import get_logger

logger = get_logger(__name__)


class EmailQueueStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"


class EmailQueueManager:
    """Redis 기반 이메일 큐 관리자"""
    
    def __init__(self):
        self.redis = redis_client()
        self.pending_queue_key = "email_queue:pending"
        self.status_key_prefix = "email_queue:status"
    
    def add_to_queue(self, conversation_id: int, email: str, user_id: int) -> str:
        """이메일 전송 요청을 큐에 추가"""
        queue_id = f"{conversation_id}_{email}_{int(time.time())}"
        
        email_request = {
            "queue_id": queue_id,
            "conversation_id": conversation_id,
            "email": email,
            "user_id": user_id,
            "created_at": int(time.time()),
            "status": EmailQueueStatus.PENDING.value
        }
        
        # 대기 큐에 추가
        self.redis.lpush(self.pending_queue_key, json.dumps(email_request))
        
        # 상태 저장
        status_key = f"{self.status_key_prefix}:{queue_id}"
        self.redis.hset(status_key, mapping=email_request)
        self.redis.expire(status_key, 86400)  # 24시간 후 만료
        
        logger.info(f"이메일 요청이 큐에 추가됨: queue_id={queue_id}, conversation_id={conversation_id}, email={email}")
        return queue_id
    
    def get_pending_requests_for_conversation(self, conversation_id: int) -> List[Dict[str, Any]]:
        """특정 대화에 대한 대기 중인 이메일 요청들을 조회"""
        pending_requests = []
        
        # 대기 큐에서 모든 요청을 확인
        queue_length = self.redis.llen(self.pending_queue_key)
        for i in range(queue_length):
            request_json = self.redis.lindex(self.pending_queue_key, i)
            if request_json:
                request = json.loads(request_json)
                if int(request.get("conversation_id")) == conversation_id:
                    pending_requests.append(request)
        
        return pending_requests
    
    def remove_from_pending_queue(self, queue_id: str) -> bool:
        """대기 큐에서 특정 요청 제거"""
        queue_length = self.redis.llen(self.pending_queue_key)
        
        for i in range(queue_length):
            request_json = self.redis.lindex(self.pending_queue_key, i)
            if request_json:
                request = json.loads(request_json)
                if request.get("queue_id") == queue_id:
                    # 찾은 요청을 큐에서 제거
                    self.redis.lrem(self.pending_queue_key, 1, request_json)
                    logger.info(f"큐에서 요청 제거: queue_id={queue_id}")
                    return True
        
        return False
    
    def update_status(self, queue_id: str, status: EmailQueueStatus, error_message: Optional[str] = None) -> None:
        """이메일 요청 상태 업데이트"""
        status_key = f"{self.status_key_prefix}:{queue_id}"
        
        updates = {
            "status": status.value,
            "updated_at": int(time.time())
        }
        
        if error_message:
            updates["error_message"] = error_message
        
        self.redis.hset(status_key, mapping=updates)
        logger.info(f"이메일 요청 상태 업데이트: queue_id={queue_id}, status={status.value}")
    
    def get_status(self, queue_id: str) -> Optional[Dict[str, Any]]:
        """이메일 요청 상태 조회"""
        status_key = f"{self.status_key_prefix}:{queue_id}"
        status_data = self.redis.hgetall(status_key)
        
        if not status_data:
            return None
        
        return status_data
    
    def process_pending_requests_for_conversation(self, conversation_id: int) -> List[str]:
        """특정 대화에 대한 모든 대기 중인 이메일 요청 처리"""
        pending_requests = self.get_pending_requests_for_conversation(conversation_id)
        processed_queue_ids = []
        
        for request in pending_requests:
            queue_id = request["queue_id"]
            
            try:
                # 큐에서 제거하고 처리 상태로 변경
                if self.remove_from_pending_queue(queue_id):
                    self.update_status(queue_id, EmailQueueStatus.PROCESSING)
                    processed_queue_ids.append(queue_id)
            except Exception as e:
                logger.error(f"이메일 요청 처리 중 오류: queue_id={queue_id}, error={str(e)}")
                self.update_status(queue_id, EmailQueueStatus.FAILED, str(e))
        
        return processed_queue_ids
    
    def mark_as_sent(self, queue_id: str) -> None:
        """이메일 전송 완료로 표시"""
        self.update_status(queue_id, EmailQueueStatus.SENT)
    
    def mark_as_failed(self, queue_id: str, error_message: str) -> None:
        """이메일 전송 실패로 표시"""
        self.update_status(queue_id, EmailQueueStatus.FAILED, error_message)


# 싱글톤 인스턴스
email_queue_manager = EmailQueueManager()