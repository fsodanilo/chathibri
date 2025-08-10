# Backend Dockerfile
FROM python:3.11-slim

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivo de dependências
COPY ../requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação backend
COPY . .

# Criar diretórios necessários
RUN mkdir -p /app/uploads
RUN mkdir -p /app/delta_datasets
RUN mkdir -p /app/temp

# Definir variáveis de ambiente
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expor porta
EXPOSE 8000

# Comando de inicialização
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
