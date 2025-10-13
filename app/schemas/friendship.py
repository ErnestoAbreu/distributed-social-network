from pydantic import BaseModel

# ✅ Para devolver amistad creada
class FriendshipOut(BaseModel):
    id: int
    user_id: int
    friend_id: int

    class Config:
        orm_mode = True
