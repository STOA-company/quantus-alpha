from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import Mapped, mapped_column

from app.models.models_base import BaseMixin, ServiceBase


class AlphafinderPostStockTag(ServiceBase, BaseMixin):
    __tablename__ = "af_post_stock_tags"

    post_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("af_posts.id", ondelete="CASCADE"), primary_key=True
    )
    stock_ticker: Mapped[String] = mapped_column(String(20), primary_key=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (Index("idx_af_post_stock_tags_ticker", "stock_ticker"),)


class AlphafinderPost(ServiceBase, BaseMixin):
    __tablename__ = "af_posts"

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    content: Mapped[Text] = mapped_column(LONGTEXT, nullable=False)
    image_url: Mapped[String] = mapped_column(LONGTEXT, nullable=True)
    image_format: Mapped[String] = mapped_column(String(20), nullable=True)
    like_count: Mapped[int] = mapped_column(Integer, default=0)
    comment_count: Mapped[int] = mapped_column(Integer, default=0)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=True)
    depth: Mapped[int] = mapped_column(Integer, default=0, comment="게시글 깊이")
    tagging_post_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=True, comment="인용된 게시글 ID")
    is_reported: Mapped[bool] = mapped_column(Boolean, default=False, comment="신고 여부")

    # Foreign Key
    parent_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("af_posts.id", ondelete="CASCADE"), nullable=True, comment="부모 게시글 ID"
    )
    category_id: Mapped[Integer] = mapped_column(Integer, ForeignKey("categories.id", ondelete="SET NULL"), nullable=True)

    __table_args__ = (
        Index("idx_af_posts_created_at", "created_at"),
        Index("idx_af_posts_like_count", "like_count"),
        Index("idx_af_posts_category_id", "category_id"),
        Index("idx_af_posts_user_id", "user_id"),
        Index("idx_af_posts_parent_id_created_at", "parent_id", "created_at"),
        Index("idx_af_posts_depth_is_reported", "depth", "is_reported"),
        Index("idx_af_posts_category_depth", "category_id", "depth"),
        Index("idx_af_posts_id_is_reported", "id", "is_reported"),
    )

    def __str__(self):
        return f"<AlphafinderPost(id={self.id}, title={self.title}, content={self.content}, image_url={self.image_url}, category_id={self.category_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<AlphafinderPost(id={self.id}, title={self.title}, content={self.content}, image_url={self.image_url}, category_id={self.category_id}, user_id={self.user_id})>"


class AlphafinderPostLike(ServiceBase, BaseMixin):
    __tablename__ = "af_post_likes"

    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False, primary_key=True)
    # Foreign keys
    is_liked: Mapped[bool] = mapped_column(Boolean, nullable=True, default=True, comment="좋아요/싫어요 여부")
    post_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("af_posts.id", ondelete="CASCADE"), nullable=False, primary_key=True
    )

    __table_args__ = (
        Index("idx_af_post_likes_user", user_id),
        Index("idx_af_post_likes_created_at_post_id", "created_at", "post_id"),
        Index("idx_af_post_likes_post_id_user_id", "post_id", "user_id"),
    )

    def __str__(self):
        return f"<AlphafinderPostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<AlphafinderPostLike(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class AlphafinderBookmark(ServiceBase, BaseMixin):
    __tablename__ = "af_bookmarks"

    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False, primary_key=True)
    # Foreign keys
    post_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("af_posts.id", ondelete="CASCADE"), nullable=False, primary_key=True
    )

    __table_args__ = (
        Index("idx_af_bookmarks_user", user_id),
        Index("idx_af_bookmarks_post_id_user_id", "post_id", "user_id"),
    )

    def __str__(self):
        return f"<AlphafinderBookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<AlphafinderBookmark(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class AlphafinderPostReport(ServiceBase, BaseMixin):
    __tablename__ = "af_post_reports"

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    post_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False)
    report_id: Mapped[Integer] = mapped_column(Integer, nullable=True)
    content: Mapped[String] = mapped_column(String(255), nullable=True)

    # __table_args__ = ( # 추후 데이터 양이 많이 지면 추가
    #     Index("idx_af_post_reports_post_id_user_id", "post_id", "user_id"),
    # )

    def __str__(self):
        return f"<AlphafinderPostReport(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"

    def __repr__(self):
        return f"<AlphafinderPostReport(id={self.id}, post_id={self.post_id}, user_id={self.user_id})>"


class AlphafinderPostReportItem(ServiceBase, BaseMixin):
    __tablename__ = "af_post_report_items"

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[String] = mapped_column(String(255), nullable=False)

    def __str__(self):
        return f"<AlphafinderPostReportItem(id={self.id}, name={self.name})>"

    def __repr__(self):
        return f"<AlphafinderPostReportItem(id={self.id}, name={self.name})>"
