from sqlalchemy import Column, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Counter(Base):
    __tablename__ = "counter"

    id = Column(Integer, primary_key=True)
    value = Column(Integer, nullable=False, default=0)
