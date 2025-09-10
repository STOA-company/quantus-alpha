import sys
from os import path
base_dir = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
sys.path.append(base_dir)

email_infos = {
    # "insight": {
    #     "email": "no-reply-insight@quantus.kr",
    #     "password": "%ngmgD6&W>RB7nP=",
    # },
    "info": {
        "email": "no-reply@quantus.kr",
        "password": "ovvflhkxucssrfvd",
    },
    "adds": {
        "email": "no-reply-ademail@quantus.kr",
        "password": "XjDYd8j*KAXH8jK>",
    },
    "adds1": {
        "email": "noreply@quantus.kr",
        "password": "iwwk pvhj cnab wmdj",
    },
    "adds2": {
        "email": "no-reply-ads@quantus.kr",
        "password": "vnlx nlae ofiz ofzx",
    },
    "adds3": {
        "email": "no-reply-ad@quantus.kr",
        "password": "gdfs rngg nzsv vwdh",
    }
}

# email_configs
email_dir = path.join(base_dir, "static/templates/email")
email_configs = {
    "verify": {
        "html_file": path.join(email_dir, "verify.html"),
        "placeholders": [
            {"code": "{{ code }}"},
        ],
        "subject": "[퀀터스] 인증번호 안내",
    },    
    "notification": {
        "html_file": path.join(email_dir, 'notification.html'),
        "placeholders": [
            {"content": "{{ content }}"},
            {"greeting": "{{ greeting }}"},
            {"closing": "{{ closing }}"},
        ],
        "subject": "[퀀터스] OO 관련 공지",   # 디폴트. 추후 수정 필요
    },
    "noti": {
        "html_file": path.join(email_dir, 'noti.html'),
        "placeholders": [
            {"content": "{{ content }}"},
        ],
        "subject": "[퀀터스] OO 관련 공지",   # 디폴트. 추후 수정 필요
    },
    # 추가사항
    ## 정책 8
    "completedPurchaseTicket": {
        "html_file": path.join(email_dir, 'completedPurchaseTicket.html'),
        "placeholders": [
            {"membership_title": "{{ membership_title }}"},   # [구독권] 매일 30게이지 1년권 (한국) / [구독권]매일 무제한 게이지 1년권 (한국) / [실전 투자권] 베이직 1년권
        ],
        "subject": "[퀀터스] 사용권 발급 완료 안내",   # 사용권 발급 완료, 디폴트 값으로 사용. 추후 수정 예정.
    },
    ## 정책 9. 실전투자 업그레이드
    "adviceUpgrade_basic_day1": {
        "html_file": path.join(email_dir, 'adviceUpgrade_basic_day1.html'),
        "placeholders": [],
        "subject": "[퀀터스 실전투자] 경고! 내일 밤 실전 투자 취소 예정",   # 실전투자 계좌총액 3,500만원 돌파 / 내일 밤 실전투자 취소예정, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "adviceUpgrade_basic_day5": {
        "html_file": path.join(email_dir, 'adviceUpgrade_basic_day5.html'),
        "placeholders": [
            {'days_left': '{{ days_left }}'},
        ],
        "subject": "[퀀터스 실전투자] 주의! 실전 투자권 업그레이드 알림",   # 실전투자 계좌총액 3,500만원 돌파 / n일 후 실전투자 취소예정, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "adviceUpgrade_basic": {
        "html_file": path.join(email_dir, 'adviceUpgrade_basic.html'),
        "placeholders": [],
        "subject": "[퀀터스 실전투자] 실전 투자권 업그레이드 알림",   # 실전투자 계좌총액 3,500만원 돌파 / 5일 후 실전투자 취소예정 ????, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "adviceUpgrade_expired": {
        "html_file": path.join(email_dir, 'adviceUpgrade_expired.html'),
        "placeholders": [
            {'membership_class': '{{ membership_class }}'},
            {'amount': '{{ amount }}'},
        ],
        "subject": "[퀀터스 실전투자] 실전 투자 취소 완료",   # 실전투자 계좌총액 일정금액 돌파 → 실전 투자 취소됨, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "adviceUpgrade_plus_day1": {
        "html_file": path.join(email_dir, 'adviceUpgrade_plus_day1.html'),
        "placeholders": [],  
        "subject": "[퀀터스 실전투자] 경고! 내일 밤 실전 투자 취소 예정",   # 실전투자 계좌총액 3,500만원 돌파 / 내일 밤 실전투자 취소예정, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "adviceUpgrade_plus_day5": {
        "html_file": path.join(email_dir, 'adviceUpgrade_plus_day5.html'),
        "placeholders": [
            {'days_left': '{{ days_left }}'},
        ],
        "subject": "[퀀터스 실전투자] 주의! 실전 투자권 업그레이드 알림",   # 실전투자 계좌총액 9천만원 돌파 / n일 후 실전투자 취소예정, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "adviceUpgrade_plus": {
        "html_file": path.join(email_dir, 'adviceUpgrade_plus.html'),
        "placeholders": [],  
        "subject": "[퀀터스 실전투자] 실전 투자권 업그레이드 알림",   # 실전투자 계좌총액 9천만원 돌파 / 5일 후 실전투자 취소예정 ????, 디폴트 값으로 사용. 추후 수정 예정.
    },

    ## 정책 10, 11. 구독권/실전 투자권 만료
    "subscription_expiration": {
        "html_file": path.join(email_dir, 'subscription_expiration.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
        ],  
        "subject": "[퀀터스] 구독권 만료 안내",   # 구독권 만료, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "subscription_expired": {
        "html_file": path.join(email_dir, 'subscription_expired.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
            {'days_in': '{{ days_in }}'}   # n 일 이내
        ],  
        "subject": "[퀀터스] 주의! 구독권 만료 D-{} 안내",   # 구독권 만료예정, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "subscription_expired_d1": {
        "html_file": path.join(email_dir, 'subscription_expired_d1.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
        ],  
        "subject": "[퀀터스] 주의! 내일 밤 구독권 만료 예정",   # 구독권 만료예정, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "trade_expiration": {
        "html_file": path.join(email_dir, 'trade_expiration.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
        ],
        "subject": "[퀀터스] 실전 투자권 만료 안내",   # 실전 투자권 만료, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "trade_expired": {
        "html_file": path.join(email_dir, 'trade_expired.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
            {'days_in': '{{ days_in }}'}   # n 일 이내
        ],  
        "subject": "[퀀터스] 실전 투자권 만료 D-{} 안내",   # 실전 투자권 만료, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "trade_expired_d2_d3": {
        "html_file": path.join(email_dir, 'trade_expired.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
            {'days_in': '{{ days_in }}'}   # n 일 이내
        ],  
        "subject": "[퀀터스] 주의! 실전 투자권 만료 D-{} 안내",   # 실전 투자권 만료, 디폴트 값으로 사용. 추후 수정 예정.
    },
    "trade_expired_d1": {
        "html_file": path.join(email_dir, 'trade_expired_d1.html'),
        "placeholders": [
            {'membership_title': '{{ membership_title }}'},
            {'expired_at': '{{ expired_at }}'},   # 2023년 7월 7일 23시 59분 59초
        ],  
        "subject": "[퀀터스] 주의! 내일 밤 실전 투자권 만료 예정",   # 실전 투자권 만료, 디폴트 값으로 사용. 추후 수정 예정.
    },
    # 추가사항 끝

    "tradeConfirm": {
        "html_file": path.join(email_dir, 'tradeConfirm.html'),
        "placeholders": [
            {"strategy_name": "{{ strategy_name }}"},
            {"iim": "{{ iim }}"},
            {"port_date": "{{ port_date }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
        ],  
        "subject": "[퀀터스 실전투자] 투자 예약 완료",
    },
    "tradeConfirmOverseas": {
        "html_file": path.join(email_dir, 'tradeConfirmOverseas.html'),
        "placeholders": [
            {"strategy_name": "{{ strategy_name }}"},
            {"iim": "{{ iim }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
        ],  
        "subject": "[퀀터스 실전투자] 투자 예약 완료",
    },
    "tradeRebalConfirm": {
        "html_file": path.join(email_dir, 'tradeRebalConfirm.html'),
        "placeholders": [
            {"rebalancing_date": "{{ rebalancing_date }}",},
            {"rebalancing_date": "{{ rebalancing_date }}",},
        ],  
        "subject": "[퀀터스 실전투자] 리밸런싱 예약 완료",
    },
    "rebalancingTriggered": {
        "html_file": path.join(email_dir, 'rebalancingTriggered.html'),
        "placeholders": [
            {"cause": "{{ cause }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
        ],  
        "subject": "[퀀터스 실전투자] 리밸런싱 예정",
    },
    "tradePortfolio": {
        "html_file": path.join(email_dir, 'tradePortfolio.html'),
        "placeholders": [
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
        ],  
        "subject": "[퀀터스 실전투자] 포트 추출 완료",
    },
    "tradePortfolioOverseas": {
        "html_file": path.join(email_dir, 'tradePortfolioOverseas.html'),
        "placeholders": [
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"trading_time": "{{ trading_time }}"},
            {"rebalancing_date": "{{ rebalancing_date }}"},
        ],  
        "subject": "[퀀터스 실전투자] 포트 추출 완료",
    },

    ################################################################
    # 실전투자 관련 에러
    "201_portFailed": {
        "html_file": path.join(email_dir, '201_portFailed.html'),
        "subject": "[퀀터스 실전투자] 포트 추출 불가능",
    },
    "202_portFailed": {
        "html_file": path.join(email_dir, '202_portFailed.html'),
        "subject": "[퀀터스 실전투자] 포트 추출 불가능",
    },
    "203_portFailed": {
        "html_file": path.join(email_dir, '203_portFailed.html'),
        "placeholders": [
            {"asset_name": "{{ asset_name }}"},
        ],
        "subject": "[퀀터스 실전투자] 포트 추출 불가능",
    },
    "204_portFailed": {
        "html_file": path.join(email_dir, '204_portFailed.html'),
        "placeholders": [
            {"asset_name": "{{ asset_name }}"},
        ],
        "subject": "[퀀터스 실전투자] 포트 추출 불가능",
    },
    "206_portFailed": {
        "html_file": path.join(email_dir, '206_portFailed.html'),
        "subject": "[퀀터스 실전투자] 포트 추출 불가능",
    },
    "500_portFailed": {
        "html_file": path.join(email_dir, '500_portFailed.html'),
        "subject": "[퀀터스 실전투자] 포트 추출 불가능",
    },
    "tradeCancel": {
        "html_file": path.join(email_dir, 'tradeCancel.html'),
        "subject": "[퀀터스 실전투자] 실전투자 관련 알림",
    },

    ################################################################
    # strategy_rebalancing, ready_trade
    "startTradeKr": {
        "html_file": path.join(email_dir, 'startTradeKr.html'),
        "subject": "[퀀터스 실전투자] 매매 시작",
    },
    "startTradeUs": {
        "html_file": path.join(email_dir, 'startTradeUs.html'),
        "subject": "[퀀터스 실전투자] 매매 시작",
        "placeholders": [
            {"trading_time": "{{ trading_time }}"},
        ],
    },
    "finishTrade": {
        "html_file": path.join(email_dir, 'finishTrade.html'),
        "subject": "[퀀터스 실전투자] 실전 투자 완료",
    },
    "champFailedMove": {
        "html_file": path.join(email_dir, 'champFailedMove.html'),
        "placeholders": [
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"stock_code": "{{ stock_code }}"},
            {"quantity": "{{ quantity }}"},
        ],
        "subject": "[퀀터스 실전투자 대회] 주식 이동 관련 안내",
    },
    "champFailedHand": {
        "html_file": path.join(email_dir, 'champFailedHand.html'),
        "placeholders": [
            {"rebalancing_date": "{{ rebalancing_date }}"},
            {"stock_code": "{{ stock_code }}"},
            {"quantity": "{{ quantity }}"},
        ],
        "subject": "[퀀터스 실전투자 대회] 손매매 관련 안내",
    },
    "adEmail": {
        "html_file": path.join(email_dir, 'adEmail.html'),
        # "placeholders": [
        #     {"url": "{{ url }}"},
        # ],
        "subject": "(광고) 퀀터스 커뮤니티 OPEN!",
    },
    "kisHtsIdError": {
        "html_file": path.join(email_dir, 'kisHtsIdError.html'),
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    "kisVerifyError": {
        "html_file": path.join(email_dir, 'kisVerifyError.html'),
        "placeholders": [
            {"account_nickname": "{{ account_nickname }}"},
        ],
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    ## Hotfix
    "hotfix_231005": {
        "html_file": path.join(email_dir, 'hotfix_231005.html'),
        "subject": "[퀀터스 실전투자] 금일 포트 추출 실패 관련 안내",
    },
    "hotfix_portFailed": {
        "html_file": path.join(email_dir, 'hotfix_portFailed.html'),
        "subject": "[퀀터스 실전투자] 금일 포트 추출 실패 관련 안내",
    },
    "hotfix_231201": {
        "html_file": path.join(email_dir, 'hotfix_231201.html'),
        "subject": "[퀀터스 실전투자] 금일 매매 관련 안내",
    },
    "hotfix_231211": {
        "html_file": path.join(email_dir, 'hotfix_231211.html'),
        "subject": "[퀀터스 실전투자] iOS앱 관련 안내",
    },
    "hotfix_240102": {
        "html_file": path.join(email_dir, 'hotfix_240102.html'),
        "subject": "[퀀터스 실전투자] 금일 매매 관련 안내",
    },
    "hotfix_240103": {
        "html_file": path.join(email_dir, 'hotfix_240103.html'),
        "subject": "[퀀터스 실전투자대회] 실전투자 탈락 관련 안내",
    },
    "hotfix_240104": {
        "html_file": path.join(email_dir, 'hotfix_240104.html'),
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    "hotfix_240215": {
        "html_file": path.join(email_dir, 'hotfix_240215.html'),
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    "hotfix_240216": {
        "html_file": path.join(email_dir, 'hotfix_240216.html'),
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    "hotfix_240217": {
        "html_file": path.join(email_dir, 'hotfix_240217.html'),
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    "hotfix_trade": {
        "html_file": path.join(email_dir, 'hotfix_trade.html'),
        "placeholders": [
            {"strategy_name": "{{ strategy_name }}"},
        ],
        "subject": "[퀀터스] 실전투자 관련 알림",
    },
    "compensation": {
        "html_file": path.join(email_dir, 'compensation.html'),
        "placeholders": [
            {"DataFrame": "{{ DataFrame }}"},
            {"final_compensation": "{{ final_compensation }}"},            
        ],
        "subject": "[퀀터스] 1/2 주문오류 보상 안내",
    },
    ## Coupon
    "magicSplitCoupon": {
        "html_file": path.join(email_dir, "magicSplitCoupon.html"),
        "placeholders": [
            {"serial_number": "{{ serial_number }}"},
        ],
        "subject": "[퀀터스] 쿠폰번호 안내",
    },
    ## Telegram
    "telegram": {
        "html_file": path.join(email_dir, "telegram.html"),
        "subject": "[퀀터스] 텔레그램 입장 안내",
    },
    "telegram_pre_notification": {
        "html_file": path.join(email_dir, "telegram_pre_notification.html"),
        "subject": "[퀀터스] 실전투자 프리미엄 텔레그램 서포트 사전 관련 안내",
    },
    "coinEarlyBird": {
        "html_file": path.join(email_dir, "coinEarlyBird.html"),
        "placeholders": [
            {"plan_1": "{{ plan_1 }}"},
            {"serial_number_1": "{{ serial_number_1 }}"},
            {"plan_2": "{{ plan_2 }}"},
            {"serial_number_2": "{{ serial_number_2 }}"},
        ],
        "subject": "[퀀터스] 코인 얼리버드 사용권 안내",
    },
    ## Custom Trade Alarm
    "trade_alarm": {
        "html_file": path.join(email_dir, "trade_alarm.html"),
        "placeholders": [
            {"content": "{{ content }}"},
            {"account_nickname": "{{ account_nickname }}"},
        ],
        "subject": "[퀀터스] 주식 실전투자 알림",
    },
    "db_accounts_register": {
        "html_file": path.join(email_dir, "db_accounts_register.html"),
        "placeholders": [
            {"content": "{{ content }}"},
        ],
        "subject": "[퀀터스] 주식 실전투자 알림",
    },
    "coin_trade_alarm": {
        "html_file": path.join(email_dir, "coin_trade_alarm.html"),
        "placeholders": [
            {"content": "{{ content }}"},
            {"account_nickname": "{{ account_nickname }}"},
        ],
        "subject": "[퀀터스] 코인 실전투자 알림",
    },
}
