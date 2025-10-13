from pydantic import BaseModel
from datetime import datetime

class PostBase(BaseModel):
    content: str

# 📝 Para crear un post
class PostCreate(PostBase):
    pass

# ✅ Para devolver post al cliente
class PostOut(PostBase):
    id: int
    user_id: int
    created_at: datetime

    model_config = {
        "from_attributes": True
    }
