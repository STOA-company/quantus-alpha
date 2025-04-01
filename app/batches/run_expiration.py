from app.database.crud import database_service as database
from app.utils.date_utils import now_kr, now_utc


def coupon_expiration():
    """쿠폰 만료 처리"""
    utc_datetime = now_utc()
    kct_date = now_kr().date()
    inactive_coupons = database._select(
        table="alphafinder_coupon_box",
        columns=["id", "expired_at"],
        coupon_status="inactive",
    )

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

    for user in active_subscriptions:
        # 만료일이 datetime이면 date로 변환
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


if __name__ == "__main__":
    subscription_expiration()
