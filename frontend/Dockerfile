# Frontend Dockerfile
FROM python:3.11-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivo de dependências (usando o requirements.txt do projeto principal)
COPY ../requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Dependências adicionais para frontend
RUN pip install --no-cache-dir \
    jinja2==3.1.2 \
    itsdangerous==2.1.2 \
    starlette==0.27.0

# Copiar código da aplicação frontend
COPY . .

# Criar diretórios necessários
RUN mkdir -p /app/uploads
RUN mkdir -p /app/static
RUN mkdir -p /app/templates

# Definir variáveis de ambiente
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expor porta
EXPOSE 8080

# Comando de inicialização
CMD ["uvicorn", "interface.app.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]
