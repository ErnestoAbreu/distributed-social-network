from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Obtiene la URL de la base de datos desde variables de entorno
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:wasd@localhost:5432/social-network"
)

# Crea el motor de conexión (engine)
engine = create_engine(DATABASE_URL)

# Crea la fábrica de sesiones
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para los modelos
Base = declarative_base()

# Dependencia para obtener sesión de base de datos en endpoints
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
