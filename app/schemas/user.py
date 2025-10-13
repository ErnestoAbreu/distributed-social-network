from pydantic import BaseModel, EmailStr

# ✅ Datos que se devuelven al cliente
class UserOut(BaseModel):
    id: int
    username: str
    email: EmailStr

    model_config = {
        "from_attributes": True
    }


# 📝 Datos que se reciben al registrar un nuevo usuario
class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str


# 🔑 Datos que se reciben al hacer login
class UserLogin(BaseModel):
    username: str
    password: str
