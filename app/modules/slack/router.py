import json

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse

from app.core.extra.SlackNotifier import SlackNotifier
from app.database.crud import database_service

slack_notifier = SlackNotifier(
    webhook_url="https://hooks.slack.com/services/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"
)

router = APIRouter()


@router.post("/report_interactivity")
async def report_interactivity(payload: str = Form(...)):
    print(payload)  # JSON 문자열
    payload_json = json.loads(payload)

    action_id = payload_json["actions"][0]["action_id"]
    post_id = payload_json["actions"][0]["value"]
    if isinstance(post_id, str):
        post_id = int(post_id)

    if action_id == "approve_report":
        # 신고처리 되었는지 확인
        reported_post = database_service._select(
            table="af_posts",
            columns=["is_reported"],
            id=post_id,
        )
        if reported_post[0].is_reported:
            slack_notifier.send_message(f"게시글 {post_id} 이미 신고처리 되었습니다.")
            return JSONResponse(content={"text": "처리 완료되었습니다."})

        # 게시글 신고 승인
        database_service._update(
            table="af_posts",
            sets={"is_reported": True},
            id=post_id,
        )
        slack_notifier.send_message(f"게시글 {post_id} 신고 승인.")
    elif action_id == "reject_report":
        slack_notifier.send_message(f"게시글 {post_id} 신고 거절.")

    return JSONResponse(content={"text": "처리 완료되었습니다."})
