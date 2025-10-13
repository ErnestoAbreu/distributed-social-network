from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint
from app.database import Base

class Friendship(Base):
    __tablename__ = "friendships"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    friend_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Impide seguir a la misma persona dos veces
    __table_args__ = (
        UniqueConstraint("user_id", "friend_id", name="unique_friendship"),
    )
