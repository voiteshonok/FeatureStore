"""``detection.alert_rules`` and ``detection.rule_features``."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, PrimaryKeyConstraint, Text, text
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from detection.models.base import Base


class AlertRule(Base):
    """Named rule identity."""

    __tablename__ = "alert_rules"
    __table_args__ = {"schema": "detection"}

    rule_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    entity: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    features: Mapped[list["RuleFeature"]] = relationship(
        "RuleFeature",
        back_populates="rule",
        lazy="raise",
    )


class RuleFeature(Base):
    """One comparison clause on a rule."""

    __tablename__ = "rule_features"
    __table_args__ = (
        PrimaryKeyConstraint("rule_id", "feature_name"),
        {"schema": "detection"},
    )

    rule_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("detection.alert_rules.rule_id"),
        nullable=False,
    )
    feature_name: Mapped[str] = mapped_column(Text, nullable=False)
    comparison: Mapped[str] = mapped_column(Text, nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)

    rule: Mapped[AlertRule] = relationship("AlertRule", back_populates="features", lazy="raise")
