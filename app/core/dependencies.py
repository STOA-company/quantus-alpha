from elasticsearch import Elasticsearch
from fastapi import Request


async def get_es_client(request: Request) -> Elasticsearch:
    return request.app.state.es
