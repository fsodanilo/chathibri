FROM python:3.11-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Criar usuário não-root com UID/GID específicos para compatibilidade com volumes
RUN groupadd -g 1000 appuser && useradd -u 1000 -g 1000 -m appuser

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos de dependências
COPY requirements.txt .

# Atualizar pip e instalar dependências Python como root
RUN pip install --no-cache-dir --upgrade pip==24.0
RUN pip install --no-cache-dir -r requirements.txt

# Criar diretórios necessários com permissões corretas
RUN mkdir -p /app/chroma_data && \
    mkdir -p /tmp/chroma_data && \
    mkdir -p /home/appuser/.cache/huggingface && \
    mkdir -p /home/appuser/.cache/torch && \
    mkdir -p /home/appuser/.cache/sentence_transformers && \
    chown -R 1000:1000 /app && \
    chown -R 1000:1000 /home/appuser && \
    chown -R 1000:1000 /tmp/chroma_data && \
    chmod -R 755 /app && \
    chmod -R 755 /home/appuser && \
    chmod -R 755 /tmp/chroma_data

# Copiar código da aplicação
COPY --chown=appuser:appuser . .

# Definir variáveis de ambiente para cache (usando HF_HOME para evitar warnings)
ENV HF_HOME=/home/appuser/.cache/huggingface
ENV TORCH_HOME=/home/appuser/.cache/torch
ENV SENTENCE_TRANSFORMERS_HOME=/home/appuser/.cache/sentence_transformers
ENV TRANSFORMERS_CACHE=/home/appuser/.cache/huggingface

# Mudar para usuário não-root
USER appuser

# Expor porta
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# Comando para iniciar a aplicação (sem reload em produção)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
