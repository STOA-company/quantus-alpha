import logging
from app.common.celery_config import CELERY_APP
from app.core.config import settings


@CELERY_APP.task(name="hello_task", ignore_result=True)
def hello_task():
    print("Hello, World!")


@CELERY_APP.task(name="us_stock_indices_batch", ignore_result=True)
def us_run_stock_indices_batch():
    try:
        us_run_stock_indices_batch()
    except Exception as e:
        logging.error(f"Error in us_run_stock_indices_batch: {str(e)}")


if __name__ == "__main__":
    CONCURRENCY = 1
    CELERY_APP.worker_main(
        argv=["worker", "--beat", f"--loglevel={settings.CELERY_LOGLEVEL}", "-n node1@%h", f"--concurrency={CONCURRENCY}"]
    )
