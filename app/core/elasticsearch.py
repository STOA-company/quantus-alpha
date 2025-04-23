from contextlib import asynccontextmanager

from elasticsearch import Elasticsearch
from fastapi import FastAPI

from app.core.logger.logger import setup_logger

logger = setup_logger(__name__)


class ElasticSearchClient:
    def __init__(self, app: FastAPI = None, **kwargs):
        self._client = None
        if app is not None:
            self.init_app(app=app, **kwargs)

    def init_app(self, app: FastAPI, **kwargs):
        es_host = kwargs.get("ES_HOST", "elasticsearch")
        es_port = kwargs.get("ES_PORT", "9200")
        es_url = f"http://{es_host}:{es_port}"

        self._client = Elasticsearch([es_url])

        # 기존 lifespan에 Elasticsearch 추가
        old_lifespan = getattr(app.router, "lifespan_context", None)

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            if self._client.ping():
                logger.info("ElasticSearch connected.")
                app.state.es = self._client

                # 기존 lifespan 실행
                if old_lifespan:
                    async with old_lifespan(app):
                        yield
                else:
                    yield

                # Shutdown
                logger.info("ElasticSearch disconnected.")
            else:
                raise Exception("ElasticSearch connection failed")

        app.router.lifespan_context = lifespan

    @property
    def client(self):
        return self._client


# 인스턴스 생성
es = ElasticSearchClient()
