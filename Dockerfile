FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

ENV CMAKE_ARGS="-DGGML_BLAS=OFF -DGGML_AVX=OFF -DGGML_AVX2=OFF -DGGML_FMA=OFF"
ENV FORCE_CMAKE=1

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
