from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Table, Text, func
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import validates

from app.models.models_base import BaseMixin, ServiceBase

post_stocks = Table(
    "post_stocks",
    ServiceBase.metadata,
    Column("post_id", Integer, ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True),
    Column("stock_ticker", String(20), primary_key=True),
    Index("idx_post_stocks_stock_ticker", "stock_ticker"),
)


class Category(ServiceBase, BaseMixin):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)

    def __str__(self):
        return f"<Category(id={self.id}, name={self.name})>"

    def __repr__(self):
        return f"<Category(id={self.id}, name={self.name})>"


class Post(ServiceBase, BaseMixin):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    content = Column(Text, nullable=False)
    image_url = Column(LONGTEXT, nullable=True)
    image_format = Column(String(20), nullable=True)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    user_id = Column(BigInteger, nullable=True)

    # Foreign Key
    category_id = Column(Integer, ForeignKey("categories.id", ondelete="SET NULL"), nullable=True)

    __table_args__ = (
        Index("idx_posts_created_at", "created_at"),
        Index("idx_posts_like_count", "like_count"),
        Index("idx_posts_category_id", "category_id"),
        Index("idx_posts_user_id", "user_id"),
    )

    @validates("stocks")
    def validate_stocks(self, key, stock):
        if hasattr(self, "stocks") and len(self.stocks) >= 3:
            raise ValueError("종목은 최대 3개까지만 등록할 수 있습니다")
        return stock

    def __str__(self):
        return f"<Post(id={self.id}, title={self.title}, content={self.content}, image_url={self.image_url}, category_id={self.category_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Post(id={self.id}, title={self.title}, content={self.content}, image_url={self.image_url}, category_id={self.category_id}, user_id={self.user_id})>"


class Comment(ServiceBase, BaseMixin):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)
    content = Column(Text, nullable=False)
    like_count = Column(Integer, default=0)
    depth = Column(Integer, default=0, comment="댓글 깊이")
    parent_id = Column(Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=True, comment="부모 댓글 ID")
    user_id = Column(BigInteger, nullable=True)

    # Foreign Key
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False)

    __table_args__ = (
        Index("idx_comments_post_id", post_id),
        Index("idx_comments_parent_id", parent_id),
        Index("idx_comments_depth_parent", depth, parent_id),
        Index("idx_comments_user_id", user_id),
    )

    def __str__(self):
        return f"<Comment(id={self.id}, content={self.content}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Comment(id={self.id}, content={self.content}, post_id={self.post_id}, user_id={self.user_id})>"


class PostLike(ServiceBase, BaseMixin):
    __tablename__ = "post_likes"

    user_id = Column(BigInteger, nullable=False, primary_key=True)
    # Foreign keys
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    is_liked = Column(Boolean, default=True, comment="좋아요/싫어요 여부")

    __table_args__ = (Index("idx_post_likes_user", user_id),)

    def __str__(self):
        return f"<PostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<PostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class CommentLike(ServiceBase, BaseMixin):
    __tablename__ = "comment_likes"

    # Foreign keys
    comment_id = Column(Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, nullable=False, primary_key=True)
    is_liked = Column(Boolean, default=True, comment="좋아요/싫어요 여부")
    __table_args__ = (Index("idx_comment_likes_user", user_id),)

    def __str__(self):
        return f"<CommentLike(id={self.id}, comment_id={self.comment_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<CommentLike(id={self.id}, comment_id={self.comment_id}, user_id={self.user_id})>"


class Bookmark(ServiceBase, BaseMixin):
    __tablename__ = "bookmarks"

    user_id = Column(BigInteger, nullable=False, primary_key=True)
    # Foreign keys
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False, primary_key=True)

    __table_args__ = (Index("idx_bookmarks_user", user_id),)

    def __str__(self):
        return f"<Bookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Bookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class PostStatistics(ServiceBase, BaseMixin):
    __tablename__ = "post_statistics"

    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True)
    daily_likes_count = Column(Integer, default=0)
    last_liked_at = Column(DateTime, default=func.utc_timestamp())

    __table_args__ = (
        Index("idx_post_statistics_daily_likes", daily_likes_count.desc()),
        Index("idx_post_statistics_last_liked", last_liked_at),
    )

    def __str__(self):
        return f"<PostStatistics(post_id={self.post_id}, daily_likes_count={self.daily_likes_count}, last_liked_at={self.last_liked_at})>"

    def __repr__(self):
        return self.__str__()


class StockStatistics(ServiceBase, BaseMixin):
    __tablename__ = "stock_statistics"

    stock_ticker = Column(String(20), primary_key=True)
    daily_post_count = Column(Integer, default=0)
    last_tagged_at = Column(DateTime, default=func.utc_timestamp())

    __table_args__ = (
        Index("idx_stock_statistics_daily_posts", daily_post_count.desc()),
        Index("idx_stock_statistics_last_tagged", last_tagged_at),
    )

    def __str__(self):
        return f"<StockStatistics(stock_ticker={self.stock_ticker}, weekly_post_count={self.weekly_post_count})>"

    def __repr__(self):
        return self.__str__()
