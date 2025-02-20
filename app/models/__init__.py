from app.models.models_base import Base, ServiceBase
from sqlalchemy.orm import relationship

from app.models.models_stock import StockInformation
from app.models.models_stock import StockFactor
from app.models.models_dividend import Dividend
from app.models.models_news import News
from app.models.models_stock_indices import StockIndices
from app.models.models_disclosure import Disclosure
from app.models.models_users import AlphafinderUser, UserStockInterest

# from app.models.models_payments import AlphafinderLicense, AlphafinderMembership, AlphafinderPaymentHistory
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
from app.models.models_factors import Factors


__all__ = [
    "Base",
    "ServiceBase",
    "relationship",
    "StockInformation",
    "StockFactor",
    "Dividend",
    "News",
    "StockIndices",
    "Disclosure",
    "AlphafinderUser",
    "UserStockInterest",
    # "AlphafinderLicense",
    # "AlphafinderMembership",
    # "AlphafinderPaymentHistory",
    "Category",
    "Post",
    "Comment",
    "PostLike",
    "CommentLike",
    "Bookmark",
    "post_stocks",
    "PostStatistics",
    "StockStatistics",
    "Factors",
]

# Category relationships
Category.posts = relationship("Post", back_populates="category")

# Post relationships
Post.category = relationship("Category", back_populates="posts")
Post.user = relationship("AlphafinderUser", back_populates="posts", passive_deletes=True)
Post.comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")
Post.likes = relationship("PostLike", back_populates="post", cascade="all, delete-orphan")
Post.bookmarks = relationship("Bookmark", back_populates="post", cascade="all, delete-orphan")
Post.stocks = relationship(
    "StockInformation",
    secondary=post_stocks,
    back_populates="posts",
    primaryjoin="Post.id == post_stocks.c.post_id",
    secondaryjoin="StockInformation.ticker == post_stocks.c.stock_ticker",
)
Post.statistics = relationship("PostStatistics", back_populates="post", uselist=False, cascade="all, delete-orphan")

# Comment relationships
Comment.post = relationship("Post", back_populates="comments")
Comment.user = relationship("AlphafinderUser", back_populates="comments", passive_deletes=True)
Comment.likes = relationship("CommentLike", back_populates="comment", cascade="all, delete-orphan")
Comment.replies = relationship("Comment", back_populates="parent", cascade="all, delete-orphan")
Comment.parent = relationship("Comment", back_populates="replies", remote_side=[Comment.id])

# PostLike relationships
PostLike.post = relationship("Post", back_populates="likes")
PostLike.user = relationship("AlphafinderUser", back_populates="post_likes", passive_deletes=True)

# CommentLike relationships
CommentLike.comment = relationship("Comment", back_populates="likes")
CommentLike.user = relationship("AlphafinderUser", back_populates="comment_likes", passive_deletes=True)

# Bookmark relationships
Bookmark.post = relationship("Post", back_populates="bookmarks")
Bookmark.user = relationship("AlphafinderUser", back_populates="bookmarks")

# PostStatistics relationships
PostStatistics.post = relationship("Post", back_populates="statistics", uselist=False)

# AlphafinderUser relationships
AlphafinderUser.posts = relationship("Post", back_populates="user", passive_deletes=True)
AlphafinderUser.comments = relationship("Comment", back_populates="user", passive_deletes=True)
AlphafinderUser.post_likes = relationship("PostLike", back_populates="user", passive_deletes=True)
AlphafinderUser.comment_likes = relationship("CommentLike", back_populates="user", passive_deletes=True)
AlphafinderUser.bookmarks = relationship("Bookmark", back_populates="user")
