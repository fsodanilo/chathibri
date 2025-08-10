# ChromaDB Service - ChatHib

Serviço dedicado para gerenciar embeddings RAG com ChromaDB para o sistema ChatHib.

## Arquitetura

- **Framework**: FastAPI
- **Vector Database**: ChromaDB (Persistent + Fallback para EphemeralClient)
- **Embeddings**: SentenceTransformers (all-MiniLM-L6-v2)
- **Storage**: Persistente com fallback para memória
- **API**: RESTful para operações CRUD em coleções e documentos

## Dependências

Todas as dependências estão listadas no arquivo `requirements.txt`.

### Principais dependências:
- `fastapi` - Framework web
- `uvicorn` - ASGI server
- `chromadb` - Vector database para embeddings
- `sentence-transformers` - Modelo de embeddings
- `numpy` - Operações numéricas
- `pydantic` - Validação de dados

## Docker

### Dockerfiles disponíveis:
- `Dockerfile` - Build básico para desenvolvimento
- `Dockerfile.prod` - Build otimizado para produção com multi-stage

### Build e teste:
```bash
# Build básico
docker build -t chathib-chromadb .

# Build produção
docker build -f Dockerfile.prod -t chathib-chromadb:prod .

# Executar localmente
docker run -p 8001:8001 -v $(pwd)/chroma_data:/app/chroma_data chathib-chromadb
```

## Configuração

### Variáveis de Ambiente:

#### Essenciais:
```bash
PYTHONPATH=/app
PYTHONUNBUFFERED=1
```

#### Diretórios:
```bash
CHROMA_DATA_PATH=/app/chroma_data  # Diretório principal
CHROMA_FALLBACK_PATH=/tmp/chroma_data  # Diretório fallback
```

### Estratégia de Fallback:
O serviço tenta usar diretórios na seguinte ordem:
1. `/app/chroma_data` (principal)
2. `/tmp/chroma_data` (fallback)
3. `/tmp/chromadb_fallback` (fallback 2)
4. `~/chroma_data` (fallback 3)
5. Cliente em memória (último recurso)

## Execução

### Desenvolvimento:
```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

### Produção:
```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --workers 2
```

## API Endpoints

### Health Check:
- `GET /` - Página inicial do serviço
- `GET /health` - Verifica saúde do serviço e ChromaDB

### Coleções:
- `GET /collections` - Listar todas as coleções
- `GET /collections/{collection_name}` - Obter informações de uma coleção
- `DELETE /collections/{collection_name}` - Deletar coleção

### Documentos:
- `POST /collections/{collection_name}/add` - Adicionar documentos à coleção
- `POST /collections/{collection_name}/query` - Fazer consultas RAG
- `DELETE /collections/{collection_name}/documents` - Deletar documentos específicos
- `POST /collections/{collection_name}/reset` - Limpar toda a coleção

### Estatísticas:
- `GET /stats` - Estatísticas gerais do serviço

### Documentação:
- `GET /docs` - Swagger UI
- `GET /redoc` - ReDoc

## Estrutura de Dados

### Modelo DocumentChunk:
```json
{
  "text": "string",
  "metadata": {},
  "chunk_id": "string (opcional)"
}
```

### Modelo QueryRequest:
```json
{
  "query": "string",
  "collection_name": "rag_documents",
  "n_results": 5,
  "where": {}
}
```

### Modelo QueryResponse:
```json
{
  "documents": ["string"],
  "metadatas": [{}],
  "distances": [0.0],
  "ids": ["string"]
}
```

## Segurança

### Usuário não-root:
- Container executa como usuário `appuser` (UID 1000)
- Diretórios com permissões adequadas (755)

### CORS:
- Configurado para aceitar todas as origens (desenvolvimento)
- Em produção, configurar origens específicas

### Volumes:
- Dados persistentes em volume Docker
- Fallback automático para diferentes diretórios

## Monitoramento

### Health Check:
```bash
curl http://localhost:8001/health
```

### Estatísticas:
```bash
curl http://localhost:8001/stats
```

### Logs:
```bash
# Container Docker
docker logs chromadb-service

# Kubernetes
kubectl logs -n chathib deployment/chromadb-service -f
```

## Testes

### Teste de conectividade:
```bash
curl http://localhost:8001/health
curl http://localhost:8001/docs
```

### Teste de adição de documento:
```bash
curl -X POST "http://localhost:8001/collections/test/add" \
  -H "Content-Type: application/json" \
  -d '[{
    "text": "Este é um documento de teste",
    "metadata": {"source": "test", "page": 1}
  }]'
```

### Teste de consulta:
```bash
curl -X POST "http://localhost:8001/collections/test/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "documento teste",
    "n_results": 3
  }'
```

### Teste de listagem de coleções:
```bash
curl http://localhost:8001/collections
```

## Deploy no Kubernetes

Veja os manifestos em `../infra/k8s/chromadb/` para deploy no EKS.

### Comando rápido:
```bash
cd ../infra/k8s/chromadb
kubectl apply -f .
```

## Desenvolvimento

### Estrutura do código:
```
chromadb_service/
├── main.py              # Aplicação principal FastAPI
├── requirements.txt     # Dependências Python
├── Dockerfile           # Build básico
├── Dockerfile.prod      # Build produção
└── README.md           # Esta documentação
```

### Funcionalidades principais:

1. **Lazy Loading**: Cliente e modelo de embeddings inicializados sob demanda
2. **Fallback Robusto**: Múltiplas opções de diretório e cliente em memória
3. **Auto-criação**: Coleções criadas automaticamente quando necessário
4. **Embeddings Automáticos**: Geração automática de embeddings usando SentenceTransformers
5. **Metadados Flexíveis**: Suporte completo a metadados personalizados

### Adicionando nova funcionalidade:
1. Definir novos modelos Pydantic
2. Implementar endpoint em `main.py`
3. Testar localmente
4. Atualizar documentação
5. Atualizar Dockerfile se necessário

## Troubleshooting

### Problemas comuns:

#### Serviço não inicia:
- Verificar logs: `docker logs <container_id>`
- Verificar permissões de diretórios
- Verificar se porta 8001 está disponível

#### Erro de inicialização ChromaDB:
- Verificar se diretório `/app/chroma_data` é gravável
- Verificar espaço em disco
- Logs mostrarão fallback para diretório alternativo

#### Erro de modelo de embeddings:
- Verificar conexão com internet (download do modelo)
- Verificar espaço em disco para cache do modelo
- Modelo será baixado automaticamente na primeira execução

#### Performance lenta:
- Verificar se está usando cliente persistente (não em memória)
- Verificar tamanho da coleção
- Considerar usar modelo de embeddings menor

### Logs úteis:
```bash
# Logs detalhados
uvicorn main:app --log-level debug

# Logs do container
docker logs -f chromadb-service

# Logs do Kubernetes
kubectl logs -n chathib deployment/chromadb-service -f
```

### Health Check detalhado:
O endpoint `/health` verifica:
- Status do cliente ChromaDB
- Conectividade com banco de dados
- Modelo de embeddings carregado
- Diretório de dados acessível

## Integração

### Com Backend:
O backend se conecta via:
```python
CHROMADB_SERVICE_URL=http://chromadb-service:8001
```

### Fluxo de dados:
1. Backend processa PDF e extrai chunks
2. Backend envia chunks para ChromaDB via `/collections/{name}/add`
3. Backend faz consultas RAG via `/collections/{name}/query`
4. ChromaDB retorna documentos relevantes com scores de similaridade

### Comunicação:
- Todas as operações via API REST
- Dados em formato JSON
- Embeddings gerados automaticamente pelo serviço
