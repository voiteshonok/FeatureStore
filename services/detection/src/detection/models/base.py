"""Declarative base. Tables are created only by infra/postgres scripts."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """ORM registry. Never call ``Base.metadata.create_all``."""
