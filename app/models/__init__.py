from app.models.models_base import Base
from sqlalchemy.orm import relationship

from app.models.models_stock import StockInformation
from app.models.models_stock import StockFactor
from app.models.models_dividend import Dividend
from app.models.models_news import News
from app.models.models_stock_indices import StockIndices
from app.models.models_disclosure import Disclosure
from app.models.models_users import AlphafinderUser, UserStockInterest
from app.models.models_payments import AlphafinderLicense, AlphafinderMembership, AlphafinderPaymentHistory
from app.models.models_community import (
    Category,
    Post,
    Comment,
    PostLike,
    CommentLike,
    Bookmark,
    PostStatistics,
    StockStatistics,
    post_stocks,
)


__all__ = [
    "Base",
    "relationship",
    "StockInformation",
    "StockFactor",
    "Dividend",
    "News",
    "StockIndices",
    "Disclosure",
    "AlphafinderUser",
    "UserStockInterest",
    "AlphafinderLicense",
    "AlphafinderMembership",
    "AlphafinderPaymentHistory",
    "Category",
    "Post",
    "Comment",
    "PostLike",
    "CommentLike",
    "Bookmark",
    "post_stocks",
    "PostStatistics",
    "StockStatistics",
]

# Category relationships
Category.posts = relationship("Post", back_populates="category")

# Post relationships
Post.category = relationship("Category", back_populates="posts")
Post.user = relationship("AlphafinderUser", back_populates="posts", passive_deletes=True)  # 유저 삭제시 게시글 유지
Post.comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")
Post.likes = relationship("PostLike", back_populates="post", cascade="all, delete-orphan")
Post.bookmarks = relationship("Bookmark", back_populates="post", cascade="all, delete-orphan")
Post.stocks = relationship(
    "StockInformation",
    secondary=post_stocks,
    back_populates="posts",
    primaryjoin="Post.id == post_stocks.c.post_id",  # 수정
    secondaryjoin="StockInformation.ticker == post_stocks.c.stock_ticker",  # 수정
    validate=lambda k: len(k) <= 3,
)
Post.statistics = relationship("PostStatistics", back_populates="post", uselist=False)

# Comment relationships
Comment.post = relationship("Post", back_populates="comments")
Comment.user = relationship("AlphafinderUser", back_populates="comments", passive_deletes=True)  # 유저 삭제시 댓글 유지
Comment.likes = relationship("CommentLike", back_populates="comment", cascade="all, delete-orphan")

# PostLike relationships
PostLike.post = relationship("Post", back_populates="likes")
PostLike.user = relationship("AlphafinderUser", back_populates="post_likes", passive_deletes=True)

# CommentLike relationships
CommentLike.comment = relationship("Comment", back_populates="likes")
CommentLike.user = relationship("AlphafinderUser", back_populates="comment_likes", passive_deletes=True)

# Bookmark relationships
Bookmark.post = relationship("Post", back_populates="bookmarks")
Bookmark.user = relationship("AlphafinderUser", back_populates="bookmarks", cascade="all, delete")

# StockInformation relationships
StockInformation.posts = relationship(
    "Post",
    secondary=post_stocks,
    back_populates="stocks",
    primaryjoin="StockInformation.ticker == post_stocks.c.stock_ticker",
    secondaryjoin="Post.id == post_stocks.c.post_id",
)
StockInformation.statistics = relationship("StockStatistics", back_populates="stock", uselist=False)

# PostStatistics relationships
PostStatistics.post = relationship("Post", back_populates="statistics", uselist=False)

# StockStatistics relationships
StockStatistics.stock = relationship("StockInformation", back_populates="statistics", uselist=False)
