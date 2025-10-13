from app.database import engine

try:
    with engine.connect() as conn:
        print("✅ Conexión a la base de datos exitosa")
except Exception as e:
    print("❌ Error al conectar a la base de datos:", e)
