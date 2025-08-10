# Frontend - ChatHib

Interface web em FastAPI para o sistema ChatHib com suporte a chat RAG, upload de PDFs, autenticação Azure AD e processamento avançado de documentos.

## Arquitetura

- **Framework**: FastAPI + Jinja2 Templates
- **Autenticação**: Azure AD OAuth 2.0
- **Interface**: HTML5 + CSS3 + JavaScript (SPA-like)
- **Comunicação**: REST API com Backend e ChromaDB
- **Upload**: Suporte a PDFs com processamento S3
- **Chat**: Interface conversacional com sistema de feedback
- **Responsivo**: Design mobile-first

## Dependências

Todas as dependências estão listadas no arquivo `requirements.txt`.

### Principais dependências:
- `fastapi` - Framework web
- `uvicorn` - ASGI server
- `jinja2` - Template engine
- `PyJWT` - Autenticação JWT
- `google-auth` - OAuth Google (compatibilidade)
- `requests` - Cliente HTTP para APIs
- `pymongo` - Suporte MongoDB (legacy)
- `python-multipart` - Upload de arquivos

## Docker

### Dockerfiles disponíveis:
- `Dockerfile` - Build básico para desenvolvimento
- `Dockerfile.prod` - Build otimizado para produção

### Build e teste:
```bash
# Build básico
docker build -t chathib-frontend .

# Build produção
docker build -f Dockerfile.prod -t chathib-frontend:prod .

# Executar localmente
docker run -p 8080:8080 chathib-frontend
```

## Configuração

### Variáveis de Ambiente Necessárias:

#### Essenciais:
```bash
PYTHONPATH=/app
PYTHONUNBUFFERED=1
```

#### Azure AD OAuth:
```bash
AZURE_CLIENT_ID=sua_azure_client_id
AZURE_CLIENT_SECRET=sua_azure_client_secret
AZURE_TENANT_ID=sua_azure_tenant_id
AZURE_REDIRECT_URI=http://localhost:8080/auth/azure/callback
```

#### Integração com Backend:
```bash
BACKEND_SERVICE_URL=http://backend-service:8000
```

#### Opcional (Legacy):
```bash
MONGODB_URI=mongodb://localhost:27017/chathib
```

## Execução

### Desenvolvimento:
```bash
cd frontend/interface/app
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

### Produção:
```bash
uvicorn main:app --host 0.0.0.0 --port 8080 --workers 2
```

## Páginas e Rotas

### Autenticação:
- `GET /login` - Página de login
- `GET /auth/azure` - Iniciar OAuth Azure AD
- `GET /auth/azure/callback` - Callback OAuth Azure AD
- `GET /logout` - Fazer logout

### Interface Principal:
- `GET /` - Página principal do chat
- `GET /health` - Health check do serviço

### Upload e Processamento:
- `POST /upload-pdf-s3` - Upload de PDF para S3
- `POST /upload-pdf-async` - Upload assíncrono de PDF
- `GET /upload-status/{task_id}` - Status de upload assíncrono
- `GET /pdf-processor` - Página de processamento avançado

### Chat e Consultas:
- `POST /chat-pdf` - Enviar mensagem para chat RAG
- `POST /feedback` - Enviar feedback sobre respostas

### Monitoramento:
- `GET /s3-status` - Status do S3
- `GET /s3-files` - Listar arquivos no S3
- `GET /envios` - Página de envios realizados

### Datasets (Avançado):
- `GET /datasets` - Página de datasets
- `POST /generate-dataset-frontend/{type}` - Gerar dataset específico
- `POST /generate-all-datasets` - Gerar todos os datasets

### Utilitários:
- `GET /exit` - Sair da aplicação

## Interface do Usuário

### Componentes Principais:

#### 1. **Sidebar Colapsável**:
- Histórico de conversas
- Processamento avançado
- Navegação rápida
- Estado persistente (localStorage)

#### 2. **Chat Interface**:
- Mensagens em tempo real
- Indicador de digitação
- Sistema de feedback
- Scroll automático
- Animações suaves

#### 3. **Sistema de Feedback**:
- Botões de like/dislike
- Comentários opcionais
- Feedback persistente
- Visual status (enviado/erro)

#### 4. **Upload de Arquivos**:
- Drag & drop
- Progress indicator
- Status em tempo real
- Validação de tipos

#### 5. **Responsividade**:
- Mobile-first design
- Sidebar colapsável em mobile
- Touch-friendly buttons
- Adaptive layouts

### Estilo Visual:
- **Tema**: Dark mode (inspirado no Gemini)
- **Cores**: Gradientes azuis e verdes
- **Tipografia**: Sans-serif moderna
- **Ícones**: SVG inline otimizados
- **Animações**: CSS transitions suaves

## Autenticação e Segurança

### Azure AD OAuth 2.0:
- Login corporativo seguro
- Tokens JWT para sessões
- Redirect automático para login
- Logout completo com limpeza de sessão

### Segurança:
- Validação de tokens em rotas protegidas
- CORS configurado adequadamente
- Upload com validação de tipos
- Sanitização de inputs

### Proteção de Rotas:
```python
# Rotas protegidas requerem autenticação
@app.get("/protected-route")
async def protected_route(request: Request, token: str = Depends(security)):
    user = verify_token(token)
    if not user:
        raise HTTPException(401, "Não autorizado")
    return {"user": user}
```

## Funcionalidades

### 1. **Chat RAG Inteligente**:
- Conversas contextuais com PDFs
- Histórico persistente
- Respostas com IA (Bedrock/Google AI)
- Sistema de feedback para melhoria contínua

### 2. **Upload Avançado**:
- Suporte a arquivos grandes
- Upload assíncrono com progress
- Integração com S3 para storage
- Processamento automático de PDFs

### 3. **Processamento de Documentos**:
- Extração de tabelas com IA
- Geração de datasets estruturados
- Análise de conteúdo avançada
- Export em múltiplos formatos

### 4. **Interface Administrativa**:
- Status de serviços
- Monitoramento de uploads
- Visualização de estatísticas
- Logs de atividades

### 5. **Experiência do Usuário**:
- Loading states informativos
- Mensagens de erro claras
- Navegação intuitiva
- Shortcuts de teclado

## Testes

### Teste manual básico:
```bash
# Teste de conectividade
curl http://localhost:8080/health

# Teste de interface
curl http://localhost:8080/
```

### Teste de upload:
```bash
curl -X POST "http://localhost:8080/upload-pdf-s3" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@test.pdf"
```

### Teste de chat:
```bash
curl -X POST "http://localhost:8080/chat-pdf" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "question=Teste de pergunta"
```

## Deploy no Kubernetes

Veja os manifestos em `../infra/k8s/frontend/` para deploy no EKS.

### Comando rápido:
```bash
cd ../infra/k8s/frontend
kubectl apply -f .
```

## Desenvolvimento

### Estrutura do código:
```
frontend/
├── Dockerfile               # Build básico
├── Dockerfile.prod          # Build produção
├── requirements.txt         # Dependências Python
├── interface/
│   ├── app/
│   │   ├── main.py          # Aplicação principal
│   │   ├── utils.py         # Funções utilitárias
│   │   ├── templates/       # Templates Jinja2
│   │   │   ├── main.html    # Página principal
│   │   │   ├── login.html   # Página de login
│   │   │   ├── index.html   # Página inicial
│   │   │   └── pdf_processor.html # Processamento avançado
│   │   ├── static/          # Arquivos estáticos
│   │   │   ├── style.css    # Estilos principais
│   │   │   └── favicon.ico  # Ícone do site
│   │   └── img/             # Imagens
│   ├── auth/                # Módulos de autenticação
│   │   ├── oauth.py         # OAuth handlers
│   │   └── routes.py        # Rotas de auth
│   └── static/              # Assets compartilhados
└── README.md               # Esta documentação
```

### Fluxo de desenvolvimento:

#### 1. **Frontend → Backend**:
```python
# Exemplo de chamada para backend
response = requests.post(
    f"{BACKEND_SERVICE_URL}/chat",
    json={
        "message": user_message,
        "pdf_name": selected_pdf,
        "user_id": user.id
    }
)
```

#### 2. **Gerenciamento de Estado**:
- Sessions para autenticação
- LocalStorage para preferências
- Cookies para tokens (httpOnly)

#### 3. **Error Handling**:
- Try/catch em todas as chamadas API
- Fallbacks para offline
- Mensagens de erro user-friendly

### Adicionando nova funcionalidade:

1. **Nova rota**:
```python
@app.get("/nova-funcionalidade")
async def nova_funcionalidade(request: Request):
    return templates.TemplateResponse("nova_pagina.html", {"request": request})
```

2. **Novo template**:
```html
<!DOCTYPE html>
<html>
<head>
    <title>Nova Funcionalidade</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <!-- Conteúdo -->
</body>
</html>
```

3. **JavaScript interativo**:
```javascript
function novaFuncionalidade() {
    fetch('/api/nova-funcionalidade', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({data: 'valor'})
    })
    .then(response => response.json())
    .then(data => {
        // Processar resposta
    });
}
```

## Troubleshooting

### Problemas comuns:

#### Erro de autenticação Azure AD:
- Verificar `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`
- Confirmar `AZURE_REDIRECT_URI` no Azure AD
- Verificar logs de OAuth

#### Interface não carrega:
- Verificar se templates existem em `/app/templates/`
- Verificar arquivos estáticos em `/app/static/`
- Verificar permissões de diretório

#### Erro de comunicação com backend:
- Verificar `BACKEND_SERVICE_URL`
- Testar conectividade: `curl $BACKEND_SERVICE_URL/health`
- Verificar logs do backend

#### Upload de arquivos falha:
- Verificar tamanho máximo permitido
- Verificar tipos de arquivo aceitos
- Verificar espaço em disco/S3

#### Chat não funciona:
- Verificar se backend está rodando
- Verificar se ChromaDB está acessível
- Verificar logs de chat no backend

### Logs úteis:
```bash
# Logs do container
docker logs frontend-service

# Logs detalhados (desenvolvimento)
uvicorn main:app --log-level debug

# Logs do Kubernetes
kubectl logs -n chathib deployment/frontend-deployment -f
```

### Debugging JavaScript:
```javascript
// Habilitar logs detalhados
console.log('DEBUG: Frontend initialized');

// Debug de formulários
document.getElementById('chat-form').addEventListener('submit', function(e) {
    console.log('Enviando:', new FormData(e.target));
});

// Debug de API calls
fetch('/api/endpoint')
    .then(response => {
        console.log('Response:', response.status, response.statusText);
        return response.json();
    })
    .then(data => console.log('Data:', data))
    .catch(error => console.error('Error:', error));
```

## Integração

### Com Backend:
- Todas as operações de chat via API REST
- Upload de arquivos via multipart/form-data
- Autenticação via tokens JWT

### Com ChromaDB:
- Indireto via Backend
- RAG queries transparentes
- Feedback para melhorar respostas

### Com Azure AD:
- OAuth 2.0 flow completo
- Tokens de acesso seguros
- Perfil do usuário corporativo

## Features Principais

### ✅ Implementadas:
- ✅ Chat RAG com PDFs
- ✅ Upload de arquivos S3
- ✅ Autenticação Azure AD
- ✅ Interface responsiva
- ✅ Sistema de feedback
- ✅ Histórico de conversas
- ✅ Processamento avançado de PDFs
- ✅ Sidebar colapsável
- ✅ Loading states
- ✅ Error handling


## Performance

### Otimizações implementadas:
- Lazy loading de componentes
- CSS minificado em produção
- Imagens otimizadas
- Caching de assets estáticos
- Compressão gzip
- Bundle splitting

### Métricas alvo:
- **First Contentful Paint**: < 1.5s
- **Largest Contentful Paint**: < 2.5s
- **Time to Interactive**: < 3.0s
- **Cumulative Layout Shift**: < 0.1
