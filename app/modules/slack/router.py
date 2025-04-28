import urllib.parse

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse

from app.core.extra.SlackNotifier import SlackNotifier
from app.database.crud import database_service

slack_notifier = SlackNotifier()

router = APIRouter()


@router.post("/report_interactivity")
async def report_interactivity(payload: str = Form(...)):
    data = urllib.parse.parse_qs(payload)
    payload_data = data.get("payload")[0]

    import json

    payload_json = json.loads(payload_data)

    action_id = payload_json["actions"][0]["action_id"]
    post_id = payload_json["actions"][0]["value"]
    if isinstance(post_id, str):
        post_id = int(post_id)

    if action_id == "approve_report":
        # 게시글 신고 승인
        database_service._update(
            table="af_post_report",
            sets={"is_reported": True},
            where={"id": post_id},
        )
        slack_notifier.send_message(f"게시글 {post_id} 신고 승인.")
    elif action_id == "reject_report":
        slack_notifier.send_message(f"게시글 {post_id} 신고 거절.")

    return JSONResponse(content={"text": "처리 완료되었습니다."})
