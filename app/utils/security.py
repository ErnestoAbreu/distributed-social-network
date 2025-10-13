from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext

# Clave secreta para firmar los JWT (en producción usa una variable de entorno)
SECRET_KEY = "SECRET_SUPER_SEGURO"  
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60  # tiempo de expiración del token (1 hora)

# Contexto para manejar contraseñas con bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ==========================
# 🔐 FUNCIONES DE CONTRASEÑA
# ==========================

def hash_password(password: str) -> str:
    """Hashea una contraseña en texto plano usando bcrypt"""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica que la contraseña en texto plano coincida con el hash"""
    return pwd_context.verify(plain_password, hashed_password)


# ==========================
# 🪙 FUNCIONES DE JWT
# ==========================

def create_access_token(data: dict, expires_delta: timedelta = None):
    """
    Genera un JWT para un usuario autenticado.
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str):
    """
    Decodifica el JWT y devuelve los datos si es válido.
    Lanza una excepción si no lo es.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
