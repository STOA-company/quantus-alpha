from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String, Table, Text, Index
from app.models.models_base import Base, BaseMixin
from sqlalchemy.orm import validates


post_stocks = Table(
    "post_stocks",
    Base.metadata,
    Column("post_id", Integer, ForeignKey("posts.id"), primary_key=True),
    Column("stock_ticker", String(20), primary_key=True),
    Index("idx_post_stocks_stock_ticker", "stock_ticker"),
)


class Category(Base, BaseMixin):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)

    def __str__(self):
        return f"<Category(id={self.id}, name={self.name})>"

    def __repr__(self):
        return f"<Category(id={self.id}, name={self.name})>"


class Post(Base, BaseMixin):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    content = Column(Text, nullable=False)
    image_url = Column(Text, nullable=True)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)

    # Foreign Key
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False)

    __table_args__ = (
        Index("idx_posts_created_at", "created_at"),
        Index("idx_posts_like_count", "like_count"),
        Index("idx_posts_category_id", "category_id"),
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


class Comment(Base, BaseMixin):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)
    content = Column(Text, nullable=False)
    like_count = Column(Integer, default=0)
    depth = Column(Integer, default=0, comment="댓글 깊이")
    parent_id = Column(Integer, ForeignKey("comments.id"), nullable=True, comment="부모 댓글 ID")

    # Foreign Key
    post_id = Column(Integer, ForeignKey("posts.id"), nullable=False)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False)

    __table_args__ = (
        Index("idx_comments_post_id", post_id),
        Index("idx_comments_parent_id", parent_id),
        Index("idx_comments_depth_parent", depth, parent_id),
    )

    def __str__(self):
        return f"<Comment(id={self.id}, content={self.content}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Comment(id={self.id}, content={self.content}, post_id={self.post_id}, user_id={self.user_id})>"


class PostLike(Base, BaseMixin):
    __tablename__ = "post_likes"

    # Foreign keys
    post_id = Column(Integer, ForeignKey("posts.id"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False, primary_key=True)

    __table_args__ = (Index("idx_post_likes_user", user_id),)

    def __str__(self):
        return f"<PostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<PostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class CommentLike(Base, BaseMixin):
    __tablename__ = "comment_likes"

    # Foreign keys
    comment_id = Column(Integer, ForeignKey("comments.id"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False, primary_key=True)

    __table_args__ = (Index("idx_comment_likes_user", user_id),)

    def __str__(self):
        return f"<CommentLike(id={self.id}, comment_id={self.comment_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<CommentLike(id={self.id}, comment_id={self.comment_id}, user_id={self.user_id})>"


class Bookmark(Base, BaseMixin):
    __tablename__ = "bookmarks"

    # Foreign keys
    post_id = Column(Integer, ForeignKey("posts.id"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False, primary_key=True)

    __table_args__ = (Index("idx_bookmarks_user", user_id),)

    def __str__(self):
        return f"<Bookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Bookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class PostStatistics(Base, BaseMixin):
    __tablename__ = "post_statistics"

    post_id = Column(Integer, ForeignKey("posts.id"), primary_key=True)
    weekly_likes = Column(Integer, default=0)

    __table_args__ = (Index("idx_post_statistics_weekly_likes", weekly_likes.desc()),)

    def __str__(self):
        return f"<PostStatistics(post_id={self.post_id}, weekly_likes={self.weekly_likes})>"

    def __repr__(self):
        return self.__str__()


class StockStatistics(Base, BaseMixin):
    __tablename__ = "stock_statistics"

    stock_ticker = Column(String(20), primary_key=True)
    weekly_post_count = Column(Integer, default=0)

    __table_args__ = (Index("idx_stock_statistics_weekly_posts", weekly_post_count.desc()),)

    def __str__(self):
        return f"<StockStatistics(stock_ticker={self.stock_ticker}, weekly_post_count={self.weekly_post_count})>"

    def __repr__(self):
        return self.__str__()
