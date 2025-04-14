from app.database.crud import database_service as database
from app.utils.date_utils import now_kr, now_utc
from app.core.logger.logger import get_logger
from app.core.extra.SlackNotifier import SlackNotifier

slack_notifier = SlackNotifier(
    webhook_url="https://hooks.slack.com/services/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"
)

logger = get_logger(__name__)


def coupon_expiration():
    """쿠폰 만료 처리"""
    utc_datetime = now_utc()
    kct_date = now_kr().date()
    inactive_coupons = database._select(
        table="alphafinder_coupon_box",
        columns=["id", "expired_at"],
        coupon_status="inactive",
    )
    logger.info(f"사용 가능한 쿠폰 갯수: {len(inactive_coupons)}개")
    slack_notifier.notify_info(f"사용 가능한 쿠폰 갯수: {len(inactive_coupons)}개")
    expired_coupons = []

    for coupon in inactive_coupons:
        if hasattr(coupon.expired_at, "date"):
            expired_date = coupon.expired_at.date()
        else:
            expired_date = coupon.expired_at

        if expired_date < kct_date:
            database._update(
                table="alphafinder_coupon_box",
                sets={"coupon_status": "expired", "updated_at": utc_datetime},
                id=coupon.id,
            )
            expired_coupons.append(coupon.id)

    logger.info(f"만료된 쿠폰: {len(expired_coupons)}개, 쿠폰 id: {expired_coupons if expired_coupons else '없음'}")
    slack_notifier.notify_info(
        f"만료된 쿠폰: {len(expired_coupons)}개, 쿠폰 id: {expired_coupons if expired_coupons else '없음'}"
    )


def subscription_expiration():
    """구독 만료 처리"""
    utc_datetime = now_utc()
    kct_date = now_kr().date()
    # 만료일이 지난 활성 구독 조회
    active_subscriptions = database._select(
        table="alphafinder_user",
        columns=["id", "subscription_end"],
        is_subscribed=True,
    )
    user_ids = [user.id for user in active_subscriptions]
    logger.info(f"구독 중인 유저 계정: {len(user_ids)}개")
    slack_notifier.notify_info(f"구독 중인 유저 계정: {len(user_ids)}개")
    expired_users = []
    for user in active_subscriptions:
        if hasattr(user.subscription_end, "date"):
            expired_date = user.subscription_end.date()
        else:
            expired_date = user.subscription_end

        # 만료일이 오늘보다 이전인 구독만 비활성화
        if expired_date < kct_date:
            database._update(
                table="alphafinder_user",
                sets={"is_subscribed": False, "subscription_level": 1, "updated_at": utc_datetime},
                id=user.id,
            )
            expired_users.append(user.id)
    logger.info(
        f"구독 만료된 유저 계정: {len(expired_users)}개, 유저 계정 id: {expired_users if expired_users else '없음'}"
    )
    slack_notifier.notify_info(
        f"구독 만료된 유저 계정: {len(expired_users)}개, 유저 계정 id: {expired_users if expired_users else '없음'}"
    )


if __name__ == "__main__":
    subscription_expiration()
