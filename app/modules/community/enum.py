from enum import Enum


class PostOrderBy(str, Enum):
    created_at = "created_at"
    like_count = "like_count"
