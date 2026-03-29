# 1. Imagen base (Python oficial)
FROM python:3.11-slim

# 2. Evita archivos .pyc y buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Directorio de trabajo dentro del contenedor
WORKDIR /app

# 4. Copiar dependencias primero (optimiza cache)
COPY requirements.txt .

# 5. Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copiar el resto del código
COPY . .

# 7. Comando que ejecuta el ETL
CMD ["python", "ETL.py"]