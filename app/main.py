from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import Base, engine
from app.routers import auth, users, posts, friendships

# Crear tablas en la base de datos (en producción esto lo haces con Alembic)
Base.metadata.create_all(bind=engine)

# Instancia de la app FastAPI
app = FastAPI(
    title="Red Social Distribuida API",
    description="API backend para una red social similar a Twitter 🚀",
    version="1.0.0"
)

# Configurar CORS (para permitir frontend en otro dominio o puerto)
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    # Aquí puedes agregar dominios de producción
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(posts.router)
app.include_router(friendships.router)

# Ruta básica para verificar que el servidor está corriendo
@app.get("/")
def root():
    return {"message": "🚀 Bienvenido a Red Social Distribuida"}

