# Imagen base ligera con Python 3.9
FROM python:3.9-slim

# Evita que Python genere archivos .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Crear directorio de trabajo
WORKDIR /code

# Instalar dependencias del sistema (para psycopg2 si usas PostgreSQL, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalarlos
COPY requirements.txt /code/
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY . /code/

# Exponer el puerto
EXPOSE 8000

# Comando para correr el servidor
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
