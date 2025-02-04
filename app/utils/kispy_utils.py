from kispy import KisAuth
from dotenv import load_dotenv
import os

load_dotenv(".env.dev")

auth = KisAuth(
    app_key=os.getenv("KIS_APP_KEY"),
    secret=os.getenv("KIS_SECRET"),
    account_no=os.getenv("KIS_ACCOUNT_NO"),
    is_real=True,  # 실전투자: True, 모의투자: False
)
