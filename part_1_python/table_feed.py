from sqlalchemy import Column, Integer, String, engine, Date, ForeignKey, TIMESTAMP

from database import Base
from sqlalchemy.orm import relationship


class Feed(Base):
    __tablename__ = "feed_action"
    action = Column(String)
    user_id = Column(Integer, ForeignKey("user.id"), primary_key=True)
    user = relationship("User")
    post_id = Column(Integer, ForeignKey("post.id"), primary_key=True)
    post = relationship("Post")
    time = Column(TIMESTAMP)
