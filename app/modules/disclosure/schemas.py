from pydantic import BaseModel


class DisclosureItem(BaseModel):
    title: str
    date: str
    summary: str | None = None
    document_url: str
