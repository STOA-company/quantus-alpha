from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String, Table, Text
from app.models.models_base import Base, BaseMixin

post_stocks = Table(
    "post_stocks",
    Base.metadata,
    Column("post_id", Integer, ForeignKey("posts.id"), primary_key=True),
    Column("stock_ticker", String(20), primary_key=True),
)


class Category(Base, BaseMixin):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __str__(self):
        return f"<Category(id={self.id}, name={self.name})>"

    def __repr__(self):
        return f"<Category(id={self.id}, name={self.name})>"


class Post(Base, BaseMixin):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    image_url = Column(Text, nullable=True)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)

    # Foreign Key
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False)

    def __str__(self):
        return f"<Post(id={self.id}, title={self.title}, content={self.content}, image_url={self.image_url}, category_id={self.category_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Post(id={self.id}, title={self.title}, content={self.content}, image_url={self.image_url}, category_id={self.category_id}, user_id={self.user_id})>"


class Comment(Base, BaseMixin):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)
    content = Column(Text, nullable=False)
    like_count = Column(Integer, default=0)

    # Foreign Key
    post_id = Column(Integer, ForeignKey("posts.id"), nullable=False)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False)

    def __str__(self):
        return f"<Comment(id={self.id}, content={self.content}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Comment(id={self.id}, content={self.content}, post_id={self.post_id}, user_id={self.user_id})>"


class PostLike(Base, BaseMixin):
    __tablename__ = "post_likes"

    # Foreign keys
    post_id = Column(Integer, ForeignKey("posts.id"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False, primary_key=True)

    def __str__(self):
        return f"<PostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<PostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class CommentLike(Base, BaseMixin):
    __tablename__ = "comment_likes"

    # Foreign keys
    comment_id = Column(Integer, ForeignKey("comments.id"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False, primary_key=True)

    def __str__(self):
        return f"<CommentLike(id={self.id}, comment_id={self.comment_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<CommentLike(id={self.id}, comment_id={self.comment_id}, user_id={self.user_id})>"


class Bookmark(Base, BaseMixin):
    __tablename__ = "bookmarks"

    # Foreign keys
    post_id = Column(Integer, ForeignKey("posts.id"), nullable=False, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False, primary_key=True)

    def __str__(self):
        return f"<Bookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<Bookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class PostStatistics(Base, BaseMixin):
    __tablename__ = "post_statistics"

    post_id = Column(Integer, ForeignKey("posts.id"), primary_key=True)
    weekly_likes = Column(Integer, default=0)

    def __str__(self):
        return f"<PostStatistics(post_id={self.post_id}, weekly_likes={self.weekly_likes})>"

    def __repr__(self):
        return self.__str__()


class StockStatistics(Base, BaseMixin):
    __tablename__ = "stock_statistics"

    stock_ticker = Column(String(20), primary_key=True)
    weekly_post_count = Column(Integer, default=0)

    def __str__(self):
        return f"<StockStatistics(stock_ticker={self.stock_ticker}, weekly_post_count={self.weekly_post_count})>"

    def __repr__(self):
        return self.__str__()
