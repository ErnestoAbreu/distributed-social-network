from pydantic import BaseModel

# ✅ Para devolver amistad creada
class FriendshipOut(BaseModel):
    id: int
    user_id: int
    friend_id: int

    model_config = {
        "from_attributes": True
    }
