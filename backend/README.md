# Backend - ChatHib

Backend em FastAPI para o sistema ChatHib com suporte a DynamoDB, ChromaDB e geração de tabelas Delta.

## Arquitetura

- **Framework**: FastAPI
- **Banco de Dados**: AWS DynamoDB
- **Vector Database**: ChromaDB
- **ML/AI**: AWS Bedrock (Claude 3.5 Sonnet), Google Generative AI, LangChain
- **PDF Processing**: PyPDF2, PyMuPDF, pdfplumber, tabula-py, tika
- **Storage**: AWS S3 para arquivos e tabelas Delta
- **Embeddings**: SentenceTransformers

## Dependências

Todas as dependências estão listadas no arquivo `requirements.txt` local.

### Principais dependências:
- `fastapi` - Framework web
- `uvicorn` - ASGI server
- `boto3` - AWS SDK para DynamoDB e S3
- `chromadb` - Cliente para ChromaDB
- `google-generativeai` - Google AI
- `langchain` - Framework para aplicações LLM
- `langchain-aws` - Integração AWS Bedrock
- `sentence-transformers` - Embeddings
- `PyPDF2`, `PyMuPDF`, `pdfplumber`, `tabula-py`, `tika` - Processamento de PDF
- `pandas`, `pyarrow` - Processamento de dados e Delta Lake

## Docker

### Dockerfiles disponíveis:
- `Dockerfile` - Build básico para desenvolvimento

### Build e teste:
```bash
# Build manual
docker build -t chathib-backend .

# Executar localmente
docker run -p 8000:8000 chathib-backend
```

## Configuração

### Variáveis de Ambiente Necessárias:

#### Essenciais:
```bash
PYTHONPATH=/app
PYTHONUNBUFFERED=1
```

#### AWS Bedrock:
```bash
USE_BEDROCK=true
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
```

#### Google AI:
```bash
GOOGLE_API_KEY=sua_chave_api_aqui
```

#### AWS DynamoDB:
```bash
AWS_REGION=ca-central-1
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key

# Nomes das tabelas
DYNAMODB_TABLE_CHAT_HISTORY=chathib-chat-history
DYNAMODB_TABLE_PDFS=chathib-pdfs
DYNAMODB_TABLE_USERS=chathib-users
DYNAMODB_TABLE_COLLECTIONS=chathib-collections
```

#### ChromaDB:
```bash
CHROMADB_SERVICE_URL=http://chromadb-service:8001
```

#### Diretórios:
```bash
UPLOAD_DIR=/app/uploads
TEMP_DIR=/app/temp
DELTA_DATASETS_DIR=/app/delta_datasets
```

## Execução

### Desenvolvimento:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Produção:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Endpoints

### Health Check:
- `GET /health` - Verifica saúde do serviço
- `GET /health/detailed` - Verificação detalhada de saúde
- `GET /health/processing` - Status de processamento

### Documentação:
- `GET /docs` - Swagger UI
- `GET /redoc` - ReDoc

### PDF Processing:
- `POST /upload-pdf` - Upload e processamento de PDF
- `GET /pdfs` - Listar PDFs processados
- `GET /pdfs_user` - Listar PDFs por usuário
- `GET /pdfs/{pdf_name}/status` - Status de um PDF específico
- `DELETE /pdfs/{pdf_name}` - Deletar PDF
- `POST /reprocess-pdf/{pdf_name}` - Reprocessar PDF
- `GET /upload-status/{task_id}` - Status de upload assíncrono

### Chat/RAG:
- `POST /chat` - Chat com RAG usando ChromaDB
- `POST /query` - Fazer perguntas sobre PDFs
- `GET /chat-history` - Histórico de conversas
- `GET /debug/chat-history` - Debug do histórico
- `GET /recent-chats` - Chats recentes

### Usuários:
- `POST /users` - Criar usuário
- `GET /users/{user_id}` - Obter usuário

### Estatísticas e Monitoramento:
- `GET /stats` - Estatísticas gerais
- `GET /available-pdfs` - PDFs disponíveis
- `GET /test-chromadb` - Testar conexão ChromaDB

### Tabelas Delta:
- `POST /create-table-from-pdf` - Criar tabela Delta a partir de PDF

## Estrutura DynamoDB

### Tabela: chathib-chat-history
```json
{
  "id": "string (PK)",
  "timestamp": "string (GSI)",
  "question": "string",
  "answer": "string",
  "user_id": "string",
  "pdf_name": "string"
}
```

### Tabela: chathib-pdfs
```json
{
  "id": "string (PK)",
  "filename": "string (GSI)",
  "upload_date": "string",
  "content": "string",
  "size": "number",
  "user_id": "string"
}
```

### Tabela: chathib-users
```json
{
  "id": "string (PK)",
  "email": "string (GSI)",
  "name": "string",
  "created_at": "string",
  "last_login": "string"
}
```

### Tabela: chathib-collections
```json
{
  "id": "string (PK)",
  "name": "string (GSI)",
  "description": "string",
  "created_at": "string",
  "document_count": "number"
}
```

## Segurança

### Usuário não-root:
- Container executa como usuário `appuser` (UID 1000)
- Diretórios com permissões corretas (755)

### Variáveis sensíveis:
- API keys via secrets do Kubernetes
- AWS credentials via IAM roles (IRSA) ou secrets

### Network:
- Comunicação interna com ChromaDB
- HTTPS para APIs externas
- CORS configurado adequadamente

## Monitoramento

### Health Check:
```bash
curl http://localhost:8000/health
```

### Logs:
```bash
# Container Docker
docker logs chathib-backend-test

# Kubernetes
kubectl logs -n chathib deployment/backend-deployment -f
```

### Métricas:
- CPU e memória via Docker stats
- Métricas do Kubernetes via metrics-server

## Testes

### Teste de conectividade:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/docs
```

### Teste de upload:
```bash
curl -X POST "http://localhost:8000/upload-pdf" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@test.pdf"
```

### Teste de chat:
```bash
curl -X POST "http://localhost:8000/chat" \
  -H "accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{"message": "Teste de pergunta", "pdf_name": "test.pdf"}'
```

## Deploy no Kubernetes

Veja os manifestos em `../infra/k8s/backend/` para deploy no EKS.

### Comando rápido:
```bash
cd ../infra/k8s/backend
bash deploy-backend-dynamodb.sh
```

## Desenvolvimento

### Estrutura do código:
```
backend/
├── main.py                    # Aplicação principal FastAPI
├── requirements.txt           # Dependências Python
├── Dockerfile                # Docker build
├── services/                  # Serviços de negócio
│   ├── chat_service.py        # Serviço de chat com LLMs
│   ├── chromadb_client.py     # Cliente ChromaDB
│   ├── dynamodb_service.py    # Serviço DynamoDB
│   ├── pdf_processing_service.py # Processamento de PDFs
│   ├── s3_pdf_processor.py    # Processador S3 com Delta Lake
│   └── db_service.py          # Serviços de banco de dados
├── api/                       # Estruturas da API
│   ├── models.py              # Modelos Pydantic
│   └── routes.py              # Definições de rotas
└── README.md                  # Esta documentação
```

### Adicionando nova funcionalidade:
1. Criar rota em `api/`
2. Implementar lógica em `services/`
3. Atualizar `main.py` se necessário
4. Testar localmente
5. Atualizar Dockerfile se necessário

## Troubleshooting

### Problemas comuns:

#### Container não inicia:
- Verificar logs: `docker logs <container_id>`
- Verificar permissões de diretórios
- Verificar variáveis de ambiente

#### Erro de conexão DynamoDB:
- Verificar AWS credentials
- Verificar região AWS
- Verificar se tabelas existem

#### Erro de conexão ChromaDB:
- Verificar se ChromaDB está rodando
- Verificar URL do serviço
- Verificar rede entre containers

#### Erro de dependências:
- Reconstruir imagem: `docker build --no-cache`
- Verificar requirements.txt
- Verificar compatibilidade de versões

### Logs úteis:
```bash
# Logs detalhados
uvicorn main:app --log-level debug

# Logs do container
docker logs -f chathib-backend-test

# Logs do Kubernetes
kubectl logs -n chathib deployment/backend-deployment -f
```
