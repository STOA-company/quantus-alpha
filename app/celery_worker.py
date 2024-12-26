from app.common.celery_config import CELERY_APP
from app.core.config import settings


@CELERY_APP.task(name="hello_task", ignore_result=True)
def hello_task():
    print("Hello, World!")


if __name__ == "__main__":
    CONCURRENCY = 1
    CELERY_APP.worker_main(
        argv=["worker", "--beat", f"--loglevel={settings.CELERY_LOGLEVEL}", "-n node1@%h", f"--concurrency={CONCURRENCY}"]
    )
