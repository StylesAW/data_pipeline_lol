FROM quay.io/astronomer/astro-runtime:12.0.0

# Instala las dependencias de Python desde el requirements.txt
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt