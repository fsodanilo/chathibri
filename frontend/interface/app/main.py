from fastapi import FastAPI, File, UploadFile, Form, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBearer
import shutil
import os
import requests
import json
from datetime import datetime
from pydantic import BaseModel
from bson import ObjectId
from typing import Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

if not os.path.exists(STATIC_DIR):
    raise RuntimeError(f"Static directory not found: {STATIC_DIR}")

# Configurações do Azure AD OAuth
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", '')
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", '')
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", '')
AZURE_REDIRECT_URI = os.getenv("AZURE_REDIRECT_URI", 'http://localhost:8080/auth/azure/callback')

# URL do Backend
BACKEND_SERVICE_URL = "http://backend-service:8000"

# Security
security = HTTPBearer(auto_error=False)

# Importações condicionais das funções utilitárias
try:
    from utils import (
        gerar_parametros,
        executar_extracao,
        listar_uploads_mongo,
        salvar_conversa,
        listar_conversas,
        listar_conversas_do_dia,
        conecta_mongodb
    )
except ImportError:
    # Funções dummy caso utils não esteja disponível
    def gerar_parametros(*args, **kwargs):
        return "dummy_params.json"
    
    def executar_extracao(param_path):
        pass
    
    def listar_uploads_mongo():
        return []
    
    def salvar_conversa(pergunta, resposta):
        pass
    
    def listar_conversas():
        return []
    
    def listar_conversas_do_dia():
        return []
    
    def conecta_mongodb():
        class DummyClient:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
            def __getitem__(self, item):
                return self
            def find(self, *args, **kwargs):
                return []
            def sort(self, *args, **kwargs):
                return self
            def limit(self, *args, **kwargs):
                return self
            def skip(self, *args, **kwargs):
                return self
            def count_documents(self, *args, **kwargs):
                return 0
            def update_one(self, *args, **kwargs):
                return self
            @property
            def matched_count(self):
                return 0

        return DummyClient()

# ----------------------------
# Funções para Gerenciar Usuários no DynamoDB
# ----------------------------
async def register_user_in_dynamodb(user_info: dict):
    """Registra ou atualiza usuário no DynamoDB"""
    try:
        print(f"DEBUG: Registrando usuário no DynamoDB: {user_info.get('name')} ({user_info.get('email')})")
        
        # Prepara dados do usuário para o backend
        user_data = {
            "name": user_info.get("displayName", user_info.get("givenName", "") + " " + user_info.get("surname", "")),
            "email": user_info.get("mail", user_info.get("userPrincipalName", "")),
            "additional_info": {
                "azure_id": user_info.get("id", ""),
                "job_title": user_info.get("jobTitle", ""),
                "department": user_info.get("department", ""),
                "login_provider": "azure",
                "last_login": datetime.now().isoformat()
            }
        }
        
        # Registra no backend (usando porta 8000 para o backend DynamoDB )
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/users"
        response = requests.post(BACKEND_URL, json=user_data, timeout=5)
        
        print(f"DEBUG: Status da resposta do backend: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            created_user = result.get("user", {})
            print(f"DEBUG: Usuário registrado com sucesso no DynamoDB: {created_user.get('user_id', 'N/A')}")
            
        else:
            print(f"DEBUG: Erro ao registrar usuário no DynamoDB: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"DEBUG: Erro inesperado ao registrar usuário: {str(e)}")
        # Não interrompe o fluxo de login se houver erro ao registrar no DynamoDB

# ----------------------------
# Classe de Autenticação Azure AD
# ----------------------------
class AzureAuth:
    @staticmethod
    def get_azure_auth_url():
        """Gera URL para autenticação Azure AD"""
        params = {
            "client_id": AZURE_CLIENT_ID,
            "redirect_uri": AZURE_REDIRECT_URI,
            "scope": "openid email profile",
            "response_type": "code",
            "response_mode": "query",
            "state": "azure_auth"
        }
        
        auth_url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/authorize?" + "&".join([f"{k}={v}" for k, v in params.items()])
        return auth_url
    
    @staticmethod
    def exchange_code_for_tokens(code: str):
        """Troca o código por tokens de acesso"""
        print(f"DEBUG AZURE: Trocando código por tokens...")
        print(f"DEBUG AZURE: Tenant ID: {AZURE_TENANT_ID}")
        print(f"DEBUG AZURE: Client ID: {AZURE_CLIENT_ID[:10]}...")
        print(f"DEBUG AZURE: Redirect URI: {AZURE_REDIRECT_URI}")
        
        token_url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
        data = {
            "client_id": AZURE_CLIENT_ID,
            "client_secret": AZURE_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": AZURE_REDIRECT_URI
        }
        
        print(f"DEBUG AZURE: Token URL: {token_url}")
        response = requests.post(token_url, data=data)
        print(f"DEBUG AZURE: Token response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"DEBUG AZURE: Tokens obtidos com sucesso")
            return result
        else:
            print(f"DEBUG AZURE: Erro ao trocar tokens: {response.text}")
            raise HTTPException(status_code=400, detail="Failed to exchange code for tokens")
    
    @staticmethod
    def get_user_info(access_token: str):
        """Obtém informações do usuário usando o token de acesso"""
        print(f"DEBUG AZURE: Obtendo informações do usuário...")
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get("https://graph.microsoft.com/v1.0/me", headers=headers)
        
        print(f"DEBUG AZURE: User info response status: {response.status_code}")
        
        if response.status_code == 200:
            user_info = response.json()
            print(f"DEBUG AZURE: User info obtido com sucesso")
            print(f"DEBUG AZURE: Display Name: {user_info.get('displayName', 'N/A')}")
            print(f"DEBUG AZURE: Email: {user_info.get('mail', user_info.get('userPrincipalName', 'N/A'))}")
            return user_info
        else:
            print(f"DEBUG AZURE: Erro ao obter user info: {response.text}")
            raise HTTPException(status_code=400, detail="Failed to get user info")

# ----------------------------
# Middleware de Autenticação
# ----------------------------
def get_current_user(request: Request) -> Optional[dict]:
    """Obtém o usuário atual da sessão"""
    try:
        # Verifica se há token nos cookies
        access_token = request.cookies.get("access_token")
        if not access_token:
            return None
        
        # Verifica se há informações do usuário nos cookies
        user_info = request.cookies.get("user_info")
        if user_info:
            user_data = json.loads(user_info)
            # Garante que o email está presente nos dados do usuário
            if not user_data.get("email"):
                # Tenta diferentes campos para o email do Azure AD
                user_data["email"] = (
                    user_data.get("mail") or 
                    user_data.get("userPrincipalName") or 
                    user_data.get("preferredUsername") or
                    ""
                )
            return user_data
        
        # Se não há user_info mas há token, tenta buscar as informações
        if access_token:
            try:
                user_data = AzureAuth.get_user_info(access_token)
                # Garante que o email está presente
                if user_data and not user_data.get("email"):
                    user_data["email"] = (
                        user_data.get("mail") or 
                        user_data.get("userPrincipalName") or 
                        user_data.get("preferredUsername") or
                        ""
                    )
                return user_data
            except:
                return None
        
        return None
    except Exception:
        return None

# ----------------------------
# Modelos para Autenticação
# ----------------------------
class UserModel(BaseModel):
    id: str
    email: str
    name: str
    picture: Optional[str] = None
    verified_email: bool = False

app = FastAPI()

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint para Kubernetes"""
    return {"status": "healthy", "service": "frontend", "timestamp": datetime.now().isoformat()}

# ----------------------------
# Rotas de Autenticação
# ----------------------------
@app.get("/login")
async def login_page(request: Request):
    """Página de login com Google"""
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/", status_code=302)
    
    azure_auth_url = AzureAuth.get_azure_auth_url()
    return templates.TemplateResponse("login.html", {
        "request": request,
        "azure_auth_url": azure_auth_url
    })

@app.get("/auth/azure")
async def azure_auth():
    """Redireciona para autenticação Azure AD"""
    auth_url = AzureAuth.get_azure_auth_url()
    return RedirectResponse(url=auth_url)

@app.get("/auth/azure/callback")
async def azure_callback(request: Request, code: str = None, error: str = None):
    """Callback do Azure AD OAuth"""
    print(f"DEBUG CALLBACK: Azure callback chamado")
    print(f"DEBUG CALLBACK: Query params: {dict(request.query_params)}")
    print(f"DEBUG CALLBACK: Code: {code}")
    print(f"DEBUG CALLBACK: Error: {error}")
    
    if error:
        print(f"DEBUG CALLBACK: Erro do Azure AD: {error}")
        return RedirectResponse(url="/login?error=access_denied", status_code=302)
    
    if not code:
        print(f"DEBUG CALLBACK: Código não encontrado")
        return RedirectResponse(url="/login?error=missing_code", status_code=302)
    
    try:
        print(f"DEBUG CALLBACK: Trocando código por tokens...")
        # Troca o código por tokens
        tokens = AzureAuth.exchange_code_for_tokens(code)
        access_token = tokens.get("access_token")
        
        print(f"DEBUG CALLBACK: Tokens recebidos: {list(tokens.keys())}")
        
        if not access_token:
            print(f"DEBUG CALLBACK: Access token não encontrado")
            return RedirectResponse(url="/login?error=no_access_token", status_code=302)
        
        print(f"DEBUG CALLBACK: Obtendo informações do usuário...")
        # Obtém informações do usuário
        user_info = AzureAuth.get_user_info(access_token)
        
        print(f"DEBUG CALLBACK: User info recebido: {list(user_info.keys())}")
        print(f"DEBUG CALLBACK: Usuário: {user_info.get('displayName', 'N/A')} ({user_info.get('mail', user_info.get('userPrincipalName', 'N/A'))})")
        
        # Registra o usuário no DynamoDB
        await register_user_in_dynamodb(user_info)
        
        # Cria resposta de redirecionamento
        response = RedirectResponse(url="/", status_code=302)
        
        print(f"DEBUG CALLBACK: Definindo cookies...")
        # Define cookies seguros
        response.set_cookie(
            key="access_token",
            value=access_token,
            max_age=3600,  # 1 hora
            httponly=True,
            secure=False,  # Mude para True em produção com HTTPS
            samesite="lax"
        )
        
        response.set_cookie(
            key="user_info",
            value=json.dumps(user_info),
            max_age=3600,  # 1 hora
            httponly=True,
            secure=False,  # Mude para True em produção com HTTPS
            samesite="lax"
        )
        
        print(f"DEBUG CALLBACK: Redirecionando para página principal...")
        return response
        
    except Exception as e:
        print(f"Erro no callback do Azure AD: {str(e)}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        return RedirectResponse(url="/login?error=callback_error", status_code=302)

@app.get("/logout")
async def logout(request: Request):
    """Logout do usuário"""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("access_token")
    response.delete_cookie("user_info")
    return response

# ----------------------------
# Histórico do chat (em memória)
# ----------------------------
chat_history = []

# Modelo para feedback
class FeedbackRequest(BaseModel):
    message_id: str
    feedback_type: int  # 0 = positivo, 1 = negativo
    comment: str = ""

# ----------------------------
# Página inicial
# ----------------------------
@app.get("/", response_class=HTMLResponse)
async def show_main(request: Request):
    """Página principal - requer autenticação"""
    # Verifica se há código do Azure AD na URL (redirecionamento incorreto)
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    
    if code and state == "azure_auth":
        print(f"DEBUG: Código do Azure AD chegou na rota raiz, redirecionando para callback")
        # Redireciona para o callback correto
        callback_url = f"/auth/azure/callback?code={code}&state={state}"
        session_state = request.query_params.get("session_state")
        if session_state:
            callback_url += f"&session_state={session_state}"
        return RedirectResponse(url=callback_url, status_code=302)
    
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    print(f" DEBUG ROTA PRINCIPAL:")
    print(f"   - Usuário autenticado: {user.get('name', 'N/A')} ({user.get('email', 'N/A')})")
    print(f"   - OBJETO USER: {user}")
    print(f"   - Query params: {dict(request.query_params)}")
    print(f"   - URL completa: {request.url}")
    
    arquivos = []
    try:
        # Buscar PDFs do backend usando o user_id correto (email)
        user_id = user.get("email", "user_default_001")  # Usar email como user_id
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/pdfs"
        params = {"user_id": user_id}
        response = requests.get(BACKEND_URL, params=params, timeout=10)
        if response.status_code == 200:
            backend_data = response.json()
            arquivos = backend_data.get("pdfs", [])
            print(f"DEBUG MAIN: Encontrados {len(arquivos)} PDFs no backend para usuário {user_id}")
        else:
            print(f"DEBUG MAIN: Erro ao buscar PDFs: {response.status_code}")
            # Fallback para MongoDB
            arquivos = listar_uploads_mongo()
    except Exception as e:
        print(f"DEBUG MAIN: Erro ao conectar com backend: {e}")
        # Fallback para MongoDB
        arquivos = listar_uploads_mongo()
    
    # Buscar conversas do usuário logado para a sidebar
    conversas = []
    try:
        user_id_for_sidebar = user.get("id", "user_default_001")  # Google ID
        user_email_for_sidebar = user.get("email", "unknown@example.com")  # Email
        
        # Tentar primeiro com email, depois com Google ID
        sidebar_history = get_chat_history_from_dynamodb(user_email_for_sidebar, limit=5)
        
        if not sidebar_history:
            print(f"DEBUG SIDEBAR: Tentando buscar com Google ID: {user_id_for_sidebar}")
            sidebar_history = get_chat_history_from_dynamodb(user_id_for_sidebar, limit=5)
        
        conversas = sidebar_history
        print(f"DEBUG SIDEBAR: Encontradas {len(conversas)} conversas para sidebar do usuário {user_email_for_sidebar}")
        
        # Se não encontrar no DynamoDB, buscar no MongoDB como fallback
        # if not conversas:
        #     conversas = listar_conversas()
        #     print(f"DEBUG SIDEBAR: Usando fallback do MongoDB: {len(conversas)} conversas")
            
    except Exception as e:
        print(f"DEBUG SIDEBAR: Erro ao buscar conversas: {e}")
        # Fallback para MongoDB
        conversas = listar_conversas()

    # Garantir que o campo data esteja no formato correto
    for c in conversas:
        if isinstance(c.get("data"), str):
            try:
                c["data"] = datetime.fromisoformat(c["data"])
            except:
                c["data"] = None

    # Verifica se há parâmetro de erro na URL
    error_param = request.query_params.get('error')
    show_error = error_param == 'empty_question'
    
    print(f"DEBUG: error_param={error_param}, show_error={show_error}")
    print(f"DEBUG: Chat history local tem {len(chat_history)} entradas")
    
    # Combinar histórico local com conversas do DynamoDB
    history_combined = []
    
    # Buscar conversas do DynamoDB via backend
    try:
        user_id_for_history = user.get("id", "user_default_001")  # Google ID
        user_email_for_history = user.get("email", "unknown@example.com")  # Email
        
        # Tentar primeiro com email, depois com Google ID
        dynamodb_history = get_chat_history_from_dynamodb(user_email_for_history, limit=20)
        
        if not dynamodb_history:
            print(f"DEBUG: Tentando buscar com Google ID: {user_id_for_history}")
            dynamodb_history = get_chat_history_from_dynamodb(user_id_for_history, limit=20)
        
        history_combined.extend(dynamodb_history)
        print(f"DEBUG: Adicionadas {len(dynamodb_history)} conversas do DynamoDB para usuário {user_email_for_history}")
    except Exception as e:
        print(f"DEBUG: Erro ao buscar conversas do DynamoDB: {e}")
    
    # Adicionar conversas do MongoDB (fallback/legacy)
    # try:
    #     mongo_history = listar_conversas_do_dia()
    #     history_combined.extend(mongo_history)
    #     print(f"DEBUG: Adicionadas {len(mongo_history)} conversas do MongoDB (legacy)")
    # except Exception as e:
    #     print(f"DEBUG: Erro ao buscar conversas do MongoDB: {e}")
    
    # Adicionar histórico local (mais recente)
    # history_combined.extend(chat_history)
    print(f"DEBUG: Total combinado: {len(history_combined)} entradas")
    
    # Ordenar por timestamp/data se disponível
    try:
        def get_sort_key(x):
            # Priorizar timestamp, depois data, depois datetime.min
            if 'timestamp' in x and x['timestamp']:
                if isinstance(x['timestamp'], datetime):
                    return x['timestamp']
                elif isinstance(x['timestamp'], str):
                    try:
                        return datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00'))
                    except:
                        pass
            if 'data' in x and x['data']:
                if isinstance(x['data'], datetime):
                    return x['data']
                elif isinstance(x['data'], str):
                    try:
                        return datetime.fromisoformat(x['data'].replace('Z', '+00:00'))
                    except:
                        pass
            return datetime.min
        
        history_combined.sort(key=get_sort_key, reverse=True)
        # Inverter a lista para que o registro mais recente fique no final
        history_combined.reverse()
        print(f"DEBUG: Histórico ordenado por timestamp/data")
    except Exception as e:
        print(f"DEBUG: Erro ao ordenar histórico: {e}")
    
    return templates.TemplateResponse("main.html", {
        "request": request,
        "question": "",
        "arquivos": arquivos,
        "conversas": conversas,
        "history": history_combined,  # Histórico combinado
        "chat_history": chat_history,  # Histórico local separado
        "show_error": show_error,
        "user": user  # Adiciona informações do usuário ao template
    })

# ----------------------------
# Upload de PDF
# ----------------------------
# @app.post("/upload")
# async def upload_pdf(
#     file: UploadFile = File(...),
#     title: str = Form(...),
#     author: str = Form(...),
#     edition: str = Form(...),
#     year: str = Form(...),
#     initial_page: int = Form(...),
#     discard: str = Form(...)
# ):
#     os.makedirs("pdfs", exist_ok=True)
#     filepath = f"pdfs/{file.filename}"

#     with open(filepath, "wb") as buffer:
#         shutil.copyfileobj(file.file, buffer)

#     param_path = gerar_parametros(
#         file.filename, title, author, edition, year, initial_page, discard
#     )
#     executar_extracao(param_path)

#     return RedirectResponse("/", status_code=302)

# ----------------------------
# Upload de PDF para MongoDB (novo backend)
# ----------------------------
# @app.post("/upload-pdf-mongo")
# async def upload_pdf_mongo(
#     file: UploadFile = File(...),
#     request: Request = None
# ):
#     """Upload PDF para o MongoDB usando o novo backend"""
#     try:
#         # URL do backend
#         BACKEND_URL = f"{BACKEND_SERVICE_URL}/upload-pdf"
        
#         # Prepara o arquivo para envio
#         file_data = await file.read()
#         files = {
#             'file': (file.filename, file_data, 'application/pdf')
#         }
        
#         # Envia para o backend
#         response = requests.post(BACKEND_URL, files=files)
        
#         if response.status_code == 200:
#             message = response.json().get("message", "PDF enviado com sucesso!")
#             # Adiciona mensagem de sucesso ao histórico local
#             chat_history.append({
#                 "role": "system",
#                 "content": f"PDF '{file.filename}' foi processado e enviado para o MongoDB com sucesso!",
#                 "timestamp": datetime.now()
#             })
#         else:
#             message = f"Erro ao enviar PDF: {response.text}"
            
#     except Exception as e:
#         message = f"Erro ao conectar com o backend: {str(e)}"
    
#     return RedirectResponse(url=f"/envios?message={message}", status_code=303)

# ----------------------------
# Upload de PDF com processamento S3 (nova implementação)
# ----------------------------
@app.post("/upload-pdf-s3")
async def upload_pdf_s3(
    file: UploadFile = File(...),
    request: Request = None
):
    """Upload PDF com extração de tabelas e salvamento no S3"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        # URL do backend S3
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/process-pdf-tables"
        
        # Prepara o arquivo e dados do usuário para envio
        file_data = await file.read()
        files = {
            'file': (file.filename, file_data, 'application/pdf')
        }
        
        data = {
            'user_id': user.get('email', 'unknown@example.com'),  # Usar email como user_id
            'target_tables': 'investimento_financeiro,valores_contrato,produtos_servicos,cronograma_pagamentos,partes_contrato'
        }
        
        print(f"Enviando PDF para processamento S3: {file.filename}")
        print(f"Tabelas alvo: {data['target_tables']}")
        
        # Envia para o backend
        response = requests.post(BACKEND_URL, files=files, data=data, timeout=120)
        
        if response.status_code == 200:
            result = response.json()
            
            # Logs compatíveis com o sistema antigo
            print(f"PDF processado com sucesso!")
            print(f"Tabelas extraídas: {result.get('tables_extracted', [])}")
            
            # Exibe logs das tabelas extraídas
            for table_name in result.get('tables_extracted', []):
                table_data = result.get('tables_data', {}).get(table_name, [])
                print(f"Tabela {table_name}: {len(table_data)} linhas extraídas")
            
            # Exibe logs dos arquivos S3
            s3_csv_files = result.get('s3_csv_files', {})
            for table_name, s3_path in s3_csv_files.items():
                print(f"Tabela {table_name} salva: {s3_path}")
            
            # Exibe logs das tabelas deltas
            s3_delta_files = result.get('s3_delta_files', {})
            if s3_delta_files:
                print(f"Gerando tabelas deltas para {file.filename}...")
                print(f"Gerando tabelas deltas para {file.filename}...")
                
                for delta_name, delta_path in s3_delta_files.items():
                    print(f"{delta_name}: salvo -> {delta_path}")

                print(f"{len(s3_delta_files)} tabelas deltas geradas")
            
            # Adiciona mensagem de sucesso ao histórico local
            success_message = f"PDF '{file.filename}' processado com sucesso! {len(result.get('tables_extracted', []))} tabelas extraídas e salvas no S3."
            
            chat_history.append({
                "role": "system",
                "content": success_message,
                "timestamp": datetime.now(),
                "type": "s3_upload",
                "s3_files": s3_csv_files,
                "delta_files": s3_delta_files
            })
            
            return result
            
        else:
            error_message = f"Erro ao processar PDF: {response.text}"
            print(f"{error_message}")
            raise HTTPException(status_code=response.status_code, detail=error_message)
            
    except requests.exceptions.Timeout:
        error_message = "Timeout: Processamento demorou muito (mais de 2 minutos)"
        print(f"{error_message}")
        raise HTTPException(status_code=408, detail=error_message)
    except requests.exceptions.ConnectionError:
        error_message = "Erro de conexão com o backend"
        print(f"{error_message}")
        raise HTTPException(status_code=503, detail=error_message)
    except Exception as e:
        error_message = f"Erro inesperado: {str(e)}"
        print(f"{error_message}")
        raise HTTPException(status_code=500, detail=error_message)

# ----------------------------
# Status da conexão S3
# ----------------------------
@app.get("/s3-status")
async def check_s3_status(request: Request):
    """Verifica status da conexão com S3"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/s3-status"
        response = requests.get(BACKEND_URL, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "s3_available": False,
                "error": response.text,
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        return {
            "s3_available": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ----------------------------
# Listar arquivos S3
# ----------------------------
@app.get("/s3-files")
async def list_s3_files(request: Request, folder: str = "csv"):
    """Lista arquivos salvos no S3"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/s3-files"
        params = {"folder": folder}
        response = requests.get(BACKEND_URL, params=params, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "files": [],
                "total_files": 0,
                "error": response.text,
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        return {
            "files": [],
            "total_files": 0,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ----------------------------
# Página de envios
# ----------------------------
@app.get("/envios", response_class=HTMLResponse)
async def show_envios(request: Request, message: str = None):
    """Página dedicada para envio de PDFs - requer autenticação"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    try:
        # Busca lista de PDFs processados no backend DynamoDB
        user_id = user.get("email", "user_default_001")  # Usar email como user_id
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/pdfs"
        params = {"user_id": user_id}
        response = requests.get(BACKEND_URL, params=params, timeout=10)
        if response.status_code == 200:
            backend_data = response.json()
            arquivos_mongo = backend_data.get("pdfs", [])
            print(f"DEBUG ENVIOS: Encontrados {len(arquivos_mongo)} PDFs no backend para usuário {user_id}")
        else:
            print(f"DEBUG ENVIOS: Erro ao buscar PDFs: {response.status_code}")
            arquivos_mongo = []
    except Exception as e:
        print(f"DEBUG ENVIOS: Erro ao conectar com backend: {e}")
        arquivos_mongo = []
    
    # Combina com arquivos locais existentes
    try:
        arquivos_locais = listar_uploads_mongo()
    except:
        arquivos_locais = []
    
    return templates.TemplateResponse("envios.html", {
        "request": request,
        "message": message,
        "arquivos": arquivos_locais,
        "arquivos_mongo": arquivos_mongo,
        "user": user
    })

# ----------------------------
# Chat com PDFs usando IA
# ----------------------------
@app.post("/chat-pdf")
async def chat_with_pdf(request: Request):
    """Chat inteligente com os PDFs usando IA - requer autenticação"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    form = await request.form()
    question = form.get("question")
    question_backup = form.get("question_backup")
    
    user_id = user.get("id", "unknown")
    user_name = user.get("name", "Usuário")
    
    print(f"DEBUG: Pergunta de {user_name} ({user_id}): '{question}' (tipo: {type(question)})")
    print(f"DEBUG: Pergunta backup: '{question_backup}' (tipo: {type(question_backup)})")
    print(f"DEBUG: Form completo: {dict(form)}")
    
    # Se o campo principal está vazio, tenta o backup
    if not question and question_backup:
        print("DEBUG: Usando campo backup")
        question = question_backup
    
    # Validação da pergunta
    if not question:
        print("DEBUG: Pergunta é None ou vazia (ambos os campos)")
        return RedirectResponse(url="/?error=empty_question", status_code=303)
    
    question = str(question).strip()
    
    if not question or len(question) < 1:
        print(f"DEBUG: Pergunta inválida após strip: '{question}'")
        return RedirectResponse(url="/?error=empty_question", status_code=303)
    
    print(f"DEBUG: Pergunta válida de {user_name}: '{question}'")
    
    # Inicializar variável para chat_id real
    real_chat_id = None
    
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/chat"
        payload = {
            "message": question,
            "user_id": user.get("email", "unknown@example.com"),  # Usar email como user_id
            "user_name": user_name,  # Adiciona nome do usuário
            "use_context": True,
            "max_context_chunks": 5
        }
        
        print(f"DEBUG: Enviando para backend: {payload}")
        
        response = requests.post(BACKEND_URL, json=payload, timeout=30)
        
        print(f"DEBUG: Status da resposta: {response.status_code}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"DEBUG: Response JSON parseado com sucesso")
                print(f"DEBUG: Keys na resposta: {list(result.keys())}")
                
                bot_response = result.get("response", "Não foi possível gerar uma resposta.")
                print(f"DEBUG: Bot response inicial: {bot_response[:100]}...")
                print(f"DEBUG: Tipo de bot_response: {type(bot_response)}")
                
                # Capturar chat_id real do backend
                metadata = result.get("metadata", {})
                real_chat_id = metadata.get("chat_id")
                print(f"DEBUG: Chat ID capturado do backend: {real_chat_id}")
                print(f"DEBUG: Metadata completo: {metadata}")
                print(f"DEBUG: Resultado completo tem keys: {list(result.keys())}")
                
                # Verificar se chat_id está em outro local da resposta
                if not real_chat_id:
                    real_chat_id = result.get("chat_id")
                    print(f"DEBUG: Chat ID alternativo encontrado: {real_chat_id}")
                
                if not real_chat_id:
                    real_chat_id = result.get("message_id")
                    print(f"DEBUG: Message ID como chat_id: {real_chat_id}")
                
                print(f"DEBUG: Chat ID final para usar: {real_chat_id}")
                
                # Adiciona informações sobre as fontes se disponível
                sources = result.get("sources")
                print(f"DEBUG: Sources encontradas: {sources}")
                print(f"DEBUG: Tipo de sources: {type(sources)}")
                
                if sources and isinstance(sources, list):
                    try:
                        print(f"DEBUG: Processando {len(sources)} sources")
                        if sources and isinstance(sources[0], dict):
                            print(f"DEBUG: Sources são dicts - processando...")
                            # Sources é uma lista de dicts - extrair nomes dos PDFs
                            source_names = []
                            for i, source in enumerate(sources):
                                pdf_name = source.get('pdf_name', 'Documento')
                                print(f"DEBUG: Source {i}: pdf_name = {pdf_name}")
                                if pdf_name not in source_names:
                                    source_names.append(pdf_name)
                            print(f"DEBUG: Source names únicos: {source_names}")
                            if source_names:
                                fonte_texto = f"\n\nFontes consultadas: {', '.join(source_names)}"
                                bot_response += fonte_texto
                                print(f"DEBUG: Adicionado fontes ao response: {fonte_texto}")
                        else:
                            print(f"DEBUG: Sources são strings - processando...")
                            # Sources é uma lista de strings
                            fonte_texto = f"\n\nFontes consultadas: {', '.join(sources)}"
                            bot_response += fonte_texto
                            print(f"DEBUG: Adicionado fontes (strings): {fonte_texto}")
                    except Exception as e:
                        print(f"DEBUG: Erro ao processar sources: {e}")
                        # Fallback seguro
                        fallback_text = f"\n\nFontes consultadas: {len(sources)} documento(s)"
                        bot_response += fallback_text
                        print(f"DEBUG: Fallback aplicado: {fallback_text}")
                
                context_chunks = result.get("context_chunks", 0)
                print(f"DEBUG: Context chunks: {context_chunks}")
                if context_chunks == 0:
                    warning_text = "\n\nNão foram encontrados documentos relevantes para sua pergunta."
                    bot_response += warning_text
                    print(f"DEBUG: Adicionado aviso: {warning_text}")
                
                print(f"DEBUG: Bot response final: {bot_response[:200]}...")
                print(f"DEBUG: Tamanho do bot response: {len(bot_response)} chars")
                
            except json.JSONDecodeError as e:
                print(f"DEBUG: Erro ao fazer parse do JSON: {e}")
                print(f"DEBUG: Response raw: {response.text[:500]}...")
                bot_response = f"Erro ao processar resposta do backend: JSON inválido"
            except Exception as e:
                print(f"DEBUG: Erro inesperado ao processar resposta: {e}")
                bot_response = f"Erro inesperado ao processar resposta: {str(e)}"
                
        else:
            print(f"DEBUG: Erro backend: Status {response.status_code}, Resposta: {response.text}")
            bot_response = f"Erro na consulta: {response.text}"
            
    except requests.exceptions.Timeout:
        print("DEBUG: Timeout na requisição")
        bot_response = "Timeout: A consulta demorou muito para responder. Tente novamente."
    except requests.exceptions.ConnectionError:
        print("DEBUG: Erro de conexão")
        bot_response = "Erro de conexão: Não foi possível conectar ao backend. Verifique se o servidor está rodando."
    except Exception as e:
        print(f"DEBUG: Erro inesperado: {str(e)}")
        bot_response = f"Erro ao conectar com o backend: {str(e)}"
    
    print(f"DEBUG: Preparando para salvar no histórico...")
    print(f"DEBUG: Question: {question}")
    print(f"DEBUG: Bot response: {bot_response[:100]}...")
    print(f"DEBUG: User ID: {user_id}")
    print(f"DEBUG: User name: {user_name}")
    
    # Atualiza histórico local com informações do usuário
    # Estrutura compatível com o template (pergunta/resposta)
    chat_entry = {
        "pergunta": question,
        "resposta": bot_response,
        "timestamp": datetime.now(),
        "type": "pdf_chat",
        "user_id": user.get("email", "unknown@example.com"),  # Usar email como user_id
        "user_name": user_name,
        "_id": real_chat_id if real_chat_id else f"local_{len(chat_history)}",  # Usar chat_id real se disponível
        "chat_id": real_chat_id  # Adicionar chat_id separadamente para facilitar recuperação
    }
    
    print(f"DEBUG: Chat entry criado com:")
    print(f"   - _id: {chat_entry['_id']}")
    print(f"   - chat_id: {chat_entry.get('chat_id', 'N/A')}")
    print(f"   - user_id: {chat_entry['user_id']}")
    
    chat_history.append(chat_entry)
    
    print(f"DEBUG: Entrada adicionada ao histórico com estrutura pergunta/resposta")
    print(f"DEBUG: Total de entradas no histórico: {len(chat_history)}")
    print(f"DEBUG: Última entrada: pergunta='{chat_entry['pergunta'][:50]}...', resposta='{chat_entry['resposta'][:50]}...'")
    
    # Salva conversa
    try:
        print(f"DEBUG: Tentando salvar conversa...")
        salvar_conversa(question, bot_response)
        print(f"DEBUG: Conversa salva com sucesso")
    except Exception as e:
        print(f"DEBUG: Erro ao salvar conversa: {e}")
    
    # Redireciona sem parâmetros de erro - pergunta válida processada com sucesso
    print(f"DEBUG: Redirecionando para página principal...")
    print(f"DEBUG: Histórico local tem {len(chat_history)} entradas antes do redirect")
    return RedirectResponse(url="/", status_code=303)

# ----------------------------
# Consulta ao backend MongoDB
# ----------------------------
# @app.post("/query-mongo")
# async def query_mongo(request: Request):
#     """Consulta documentos no MongoDB via backend"""
#     form = await request.form()
#     question = form.get("question")
#     top_k = int(form.get("top_k", 3))
    
#     try:
#         BACKEND_URL = f"{BACKEND_SERVICE_URL}/query"
#         payload = {
#             "question": question,
#             "top_k": top_k
#         }
        
#         response = requests.post(BACKEND_URL, json=payload)
        
#         if response.status_code == 200:
#             result = response.json()
            
#             # Se temos resposta da IA, usa ela como principal
#             if result.get("ai_response"):
#                 bot_response = "Resposta da IA:\n\n" + result["ai_response"]
                
#                 # Adiciona detalhes dos documentos encontrados
#                 if result.get("context"):
#                     bot_response += f"\n\nDocumentos consultados ({len(result['context'])}):\n"
#                     for i, chunk in enumerate(result['context'][:3], 1):
#                         bot_response += f"{i}. {chunk['text'][:150]}...\n"
#                         bot_response += f"   {chunk['metadata']['filename']} (p.{chunk['metadata']['page']}) - Similaridade: {chunk['similaridade']:.3f}\n\n"
#             else:
#                 # Fallback para resposta tradicional
#                 bot_response = f"Encontrei {len(result['context'])} documentos relevantes:\n\n"
#                 for i, chunk in enumerate(result['context'], 1):
#                     bot_response += f"{i}. {chunk['text'][:200]}...\n"
#                     bot_response += f"   {chunk['metadata']['filename']} (p.{chunk['metadata']['page']}) - Similaridade: {chunk['similaridade']:.3f}\n\n"
#         else:
#             bot_response = f"Erro na consulta: {response.text}"
            
#     except Exception as e:
#         bot_response = f"Erro ao conectar com o backend: {str(e)}"
    
#     # Atualiza histórico local
#     chat_history.append({
#         "role": "user",
#         "content": question,
#         "timestamp": datetime.now(),
#         "type": "document_query"
#     })
#     chat_history.append({
#         "role": "bot", 
#         "content": bot_response,
#         "timestamp": datetime.now(),
#         "type": "document_query"
#     })
    
#     return RedirectResponse(url="/envios", status_code=303)

# ----------------------------
# Página de datasets
# ----------------------------
@app.get("/datasets", response_class=HTMLResponse)
async def show_datasets(request: Request, message: str = None):
    """Página para gerar e gerenciar datasets"""
    try:
        # Busca status dos dados no backend
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/dataset-status"
        response = requests.get(BACKEND_URL)
        
        if response.status_code == 200:
            status_data = response.json()
        else:
            status_data = {"status": "error", "message": "Erro ao conectar com backend"}
    except:
        status_data = {"status": "error", "message": "Erro de conexão"}
    
    return templates.TemplateResponse("datasets.html", {
        "request": request,
        "message": message,
        "status_data": status_data
    })

@app.post("/generate-dataset-frontend/{dataset_type}")
async def generate_dataset_frontend(dataset_type: str, request: Request):
    """Proxy para gerar dataset específico"""
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/generate-dataset/{dataset_type}"
        response = requests.post(BACKEND_URL)
        
        if response.status_code == 200:
            result = response.json()
            message = f"{result['message']} - {result['rows']} linhas, {result['columns']} colunas"
        else:
            message = f"Erro: {response.text}"
            
    except Exception as e:
        message = f"Erro de conexão: {str(e)}"
    
    return RedirectResponse(url=f"/datasets?message={message}", status_code=303)

@app.post("/generate-all-datasets")
async def generate_all_datasets_frontend(request: Request):
    """Proxy para gerar todos os datasets"""
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/generate-datasets"
        response = requests.post(BACKEND_URL)
        
        if response.status_code == 200:
            result = response.json()
            datasets = ", ".join(result['datasets_generated'])
            message = f"{result['message']} Datasets: {datasets}"
        else:
            message = f"Erro: {response.text}"
            
    except Exception as e:
        message = f"Erro de conexão: {str(e)}"
    
    return RedirectResponse(url=f"/datasets?message={message}", status_code=303)

# ----------------------------
# Status do processamento (para barra de progresso)
# ----------------------------
@app.get("/upload-status/{task_id}")
async def get_upload_status(task_id: str):
    """Retorna o status do processamento do PDF do backend"""
    try:
        # Chama o backend real
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/upload-status/{task_id}"
        response = requests.get(BACKEND_URL, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            # Task ID não encontrado
            return {
                "task_id": task_id,
                "status": "not_found",
                "progress": 0,
                "message": "Task não encontrada"
            }
        else:
            # Erro no backend
            return {
                "task_id": task_id,
                "status": "error",
                "progress": 0,
                "message": "Erro ao verificar status no backend"
            }
            
    except Exception as e:
        print(f"Erro ao conectar com backend: {e}")
        return {
            "task_id": task_id,
            "status": "error",
            "progress": 0,
            "message": f"Erro de conexão: {str(e)}"
        }

# ----------------------------
# Upload assíncrono com tracking
# ----------------------------
@app.post("/upload-pdf-async")
async def upload_pdf_async(
    file: UploadFile = File(...),
    request: Request = None
):
    """Upload PDF assíncrono com tracking de progresso"""
    try:
        # URL do backend
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/upload-pdf"
        
        # Prepara o arquivo para envio
        file_data = await file.read()
        files = {
            'file': (file.filename, file_data, 'application/pdf')
        }
        
        # Envia para o backend
        response = requests.post(BACKEND_URL, files=files, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            # Retorna o task_id real do backend
            return {
                "task_id": result.get("task_id", "unknown"),
                "status": "processing",
                "message": result.get("message", "PDF enviado para processamento"),
                "progress": 10,
                "pdf_name": result.get("pdf_name", file.filename)
            }
        else:
            return {
                "task_id": None,
                "status": "error",
                "message": f"Erro ao processar PDF: {response.text}",
                "progress": 0
            }
            
    except Exception as e:
        return {
            "task_id": None,
            "status": "error",
            "message": f"Erro ao conectar com o backend: {str(e)}",
            "progress": 0
        }

# ----------------------------
# Encerrar app (opcional)
# ----------------------------
@app.get("/exit")
async def exit_app():
    return HTMLResponse(content="Aplicação encerrada. Obrigado por usar o chathib!")

# ----------------------------
# Página para processamento de PDFs
# ----------------------------
@app.get("/pdf-processor")
async def pdf_processor_page(request: Request):
    """Página para processamento avançado de PDFs"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    # Buscar PDFs do backend
    arquivos = []
    try:
        user_id = user.get("email", "user_default_001")  # Usar email como user_id
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/pdfs"
        params = {"user_id": user_id}
        response = requests.get(BACKEND_URL, params=params, timeout=10)
        if response.status_code == 200:
            backend_data = response.json()
            arquivos = backend_data.get("pdfs", [])
            print(f"DEBUG PDF_PROCESSOR: Encontrados {len(arquivos)} PDFs no backend para usuário {user_id}")
        else:
            print(f"DEBUG PDF_PROCESSOR: Erro ao buscar PDFs: {response.status_code}")
    except Exception as e:
        print(f"DEBUG PDF_PROCESSOR: Erro ao conectar com backend: {e}")

    return templates.TemplateResponse("pdf_processor.html", {
        "request": request,
        "arquivos": arquivos,
        "user": user
    })

# ----------------------------
# Endpoint para feedback de respostas
# ----------------------------
@app.post("/feedback")
async def submit_feedback(feedback: FeedbackRequest, request: Request):
    """Recebe feedback das respostas do chatbot e salva no DynamoDB via backend"""
    try:
        # Obter usuário autenticado
        user = get_current_user(request)
        if not user:
            return JSONResponse(
                status_code=401,
                content={"success": False, "message": "Usuário não autenticado"}
            )
        
        user_id = user.get("id", "user_default_001")
        user_email = user.get("email", "unknown@example.com")
        print(f"DEBUG FEEDBACK: Usuário autenticado: {user.get('name', 'N/A')} ({user_email}) - Google ID: {user_id}")
        print(f"DEBUG FEEDBACK: Message ID recebido: {feedback.message_id}")
        
        # Para o DynamoDB, usar o email como identificador único ou o Google ID
        # O backend DynamoDB pode estar usando email como chave primária
        dynamodb_user_id = user_email  # Usar email como identificador no DynamoDB
        
        print(f"DEBUG FEEDBACK: DynamoDB User ID: {dynamodb_user_id}")
        
        # Se o message_id é local_, buscar o chat_id real no DynamoDB pela pergunta/resposta
        actual_message_id = feedback.message_id
        
        if feedback.message_id.startswith("local_"):
            # Buscar no histórico local a pergunta/resposta correspondente
            try:
                index = int(feedback.message_id.replace("local_", ""))
                if 0 <= index < len(chat_history):
                    local_chat = chat_history[index]
                    pergunta = local_chat.get("pergunta", "")
                    resposta = local_chat.get("resposta", "")
                    chat_user_id = local_chat.get("user_id", "")
                    stored_chat_id = local_chat.get("chat_id")  # Verificar se já tem chat_id real
                    
                    print(f"DEBUG FEEDBACK: Buscando no histórico local[{index}]:")
                    print(f"   - Pergunta: '{pergunta[:50]}...'")
                    print(f"   - Resposta: '{resposta[:50]}...'")
                    print(f"   - User ID do chat local: {chat_user_id}")
                    print(f"   - Chat ID armazenado: {stored_chat_id}")
                    print(f"   - User ID atual: {user_id}")
                    print(f"   - User Email atual: {user_email}")
                    print(f"   - DynamoDB User ID: {dynamodb_user_id}")
                    
                    # Verificar se o chat local pertence ao usuário atual
                    # Aceitar tanto o email quanto o Google ID do usuário como válidos
                    user_owns_chat = (
                        not chat_user_id or  # Se não tem user_id, permitir (compatibilidade)
                        chat_user_id == user_id or  # Google ID igual
                        chat_user_id == user_email or  # Email igual
                        chat_user_id == dynamodb_user_id  # DynamoDB ID igual
                    )
                    
                    print(f"DEBUG FEEDBACK: Verificação de propriedade do chat: {user_owns_chat}")
                    
                    if not user_owns_chat:
                        print(f"DEBUG FEEDBACK: Chat local pertence a outro usuário ({chat_user_id}), não ao usuário atual ({user_id}/{user_email})")
                        return JSONResponse(
                            status_code=403,
                            content={"success": False, "message": "Você não pode dar feedback em mensagens de outros usuários"}
                        )
                    
                    # Se já tem chat_id real armazenado, usar ele diretamente
                    if stored_chat_id and not stored_chat_id.startswith("local_"):
                        print(f"DEBUG FEEDBACK: Usando chat_id real já armazenado: {stored_chat_id}")
                        actual_message_id = stored_chat_id
                    else:
                        # Verificar se o _id já é um chat_id real
                        local_id = local_chat.get("_id", "")
                        print(f"DEBUG FEEDBACK: Local _id: {local_id}")
                        
                        if local_id and not local_id.startswith("local_"):
                            print(f"DEBUG FEEDBACK: Usando _id como chat_id real: {local_id}")
                            actual_message_id = local_id
                        else:
                            # Buscar no DynamoDB pelo conteúdo da pergunta/resposta com user_id real
                            # Tentar primeiro com email, depois com Google ID
                            print(f"DEBUG FEEDBACK: Chat_id não encontrado localmente, buscando no DynamoDB...")
                            dynamodb_history = get_chat_history_from_dynamodb(dynamodb_user_id, limit=50)
                        
                        # Se não encontrar com email, tentar com Google ID
                        if not dynamodb_history:
                            print(f"DEBUG FEEDBACK: Tentando buscar com Google ID: {user_id}")
                            dynamodb_history = get_chat_history_from_dynamodb(user_id, limit=50)
                            if dynamodb_history:
                                dynamodb_user_id = user_id  # Usar Google ID se encontrou resultados
                                print(f"DEBUG FEEDBACK: Encontrado histórico com Google ID, atualizando dynamodb_user_id para: {dynamodb_user_id}")
                        
                        print(f"DEBUG FEEDBACK: Encontradas {len(dynamodb_history)} conversas no DynamoDB para usuário {dynamodb_user_id}")
                        
                        # Encontrar chat correspondente pela pergunta e usuário
                        found_match = False
                        for i, dynamo_chat in enumerate(dynamodb_history):
                            dynamo_user_id = dynamo_chat.get("user_id", "")
                            dynamo_pergunta = dynamo_chat.get("pergunta", "").strip()
                            dynamo_id = dynamo_chat.get("_id", "")
                            
                            print(f"DEBUG FEEDBACK: Verificando conversa {i}: pergunta='{dynamo_pergunta[:50]}...', user_id={dynamo_user_id}, _id={dynamo_id}")
                            
                            # Comparar pergunta e verificar se user_id é compatível
                            pergunta_match = dynamo_pergunta == pergunta.strip()
                            user_match = (dynamo_user_id == dynamodb_user_id or dynamo_user_id == user_id or dynamo_user_id == user_email)
                            id_valid = dynamo_id and not dynamo_id.startswith("local_")
                            
                            print(f"   - Pergunta match: {pergunta_match}")
                            print(f"   - User match: {user_match} (dynamo: {dynamo_user_id}, current: {dynamodb_user_id})")
                            print(f"   - ID válido: {id_valid}")
                            
                            if pergunta_match and user_match and id_valid:
                                actual_message_id = dynamo_id
                                print(f"DEBUG FEEDBACK: Mapeado {feedback.message_id} -> {actual_message_id} para usuário {dynamodb_user_id}")
                                
                                # Atualizar o histórico local com o chat_id real encontrado
                                chat_history[index]["chat_id"] = actual_message_id
                                print(f"DEBUG FEEDBACK: Chat_id atualizado no histórico local: {actual_message_id}")
                                
                                found_match = True
                                break
                        
                            # Se não encontrar no DynamoDB, permitir usar o chat_id local se for válido
                            print(f"DEBUG FEEDBACK: Nenhuma conversa correspondente encontrada no DynamoDB para usuário {dynamodb_user_id}")
                            print(f"DEBUG FEEDBACK: Pergunta procurada: '{pergunta[:100]}...'")
                            if dynamodb_history:
                                print(f"DEBUG FEEDBACK: Primeira conversa no DynamoDB: '{dynamodb_history[0].get('pergunta', '')[:100]}...'")
                                print(f"DEBUG FEEDBACK: User ID da primeira conversa: {dynamodb_history[0].get('user_id', 'N/A')}")
                            else:
                                print(f"DEBUG FEEDBACK: Histórico DynamoDB vazio para usuário {dynamodb_user_id}")
                            
                            # Tentar usar o _id local como fallback se for válido
                            if local_id and not local_id.startswith("local_"):
                                print(f"DEBUG FEEDBACK: Usando _id local como fallback: {local_id}")
                                actual_message_id = local_id
                            else:
                                # Se não encontrar no DynamoDB, não é possível dar feedback
                                print(f"DEBUG FEEDBACK: Sem chat_id real disponível, não é possível enviar feedback")
                                return JSONResponse(
                                    status_code=404,
                                    content={"success": False, "message": "Mensagem não encontrada no DynamoDB. Não é possível enviar feedback."}
                                )
                else:
                    print(f"DEBUG FEEDBACK: Índice {index} fora do range do histórico local ({len(chat_history)} itens)")
                    
            except Exception as e:
                print(f"DEBUG FEEDBACK: Erro ao mapear message_id: {e}")
        
        # Se não é local_, usar o ID do usuário autenticado diretamente
        if not feedback.message_id.startswith("local_"):
            # Se o ID não é local, usar o email como identificador do usuário para buscar no DynamoDB
            dynamodb_user_id = user_email
        
        # Preparar dados para enviar ao backend
        feedback_payload = {
            "message_id": actual_message_id,
            "feedback_type": feedback.feedback_type,
            "comment": feedback.comment.strip() if feedback.comment else "",
            "user_id": dynamodb_user_id  # Usar o ID correto do DynamoDB
        }
        
        print(f"DEBUG FEEDBACK: Enviando feedback para backend:")
        print(f"   - message_id: {actual_message_id}")
        print(f"   - feedback_type: {feedback.feedback_type}")
        print(f"   - comment: '{feedback.comment[:50]}...'")
        print(f"   - user_id: {dynamodb_user_id}")
        print(f"   - chat_id valor final: {actual_message_id}")
        print(f"   - chat_id tipo: {type(actual_message_id)}")
        print(f"   - chat_id é válido: {actual_message_id and not str(actual_message_id).startswith('local_')}")
        
        # Enviar feedback para o backend (DynamoDB)
        try:
            response = requests.post(
                f"{BACKEND_SERVICE_URL}/feedback",
                json=feedback_payload,
                timeout=10
            )
            
            print(f"DEBUG FEEDBACK: Status da resposta do backend: {response.status_code}")
            
            if response.status_code == 200:
                backend_result = response.json()
                feedback_text = "positivo" if feedback.feedback_type == 0 else "negativo"
                print(f"Feedback {feedback_text} salvo no DynamoDB via backend - ID: {backend_result.get('feedback_id')}")
                
                return JSONResponse(
                    status_code=200,
                    content={
                        "success": True, 
                        "message": "Feedback salvo com sucesso no DynamoDB!",
                        "feedback_type": feedback_text,
                        "feedback_id": backend_result.get('feedback_id')
                    }
                )
            else:
                # Fallback para MongoDB se backend falhar
                print(f"Backend retornou erro {response.status_code}, tentando MongoDB como fallback")
                return await submit_feedback_mongodb_fallback(feedback)
                
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão com backend: {e}, usando MongoDB como fallback")
            return await submit_feedback_mongodb_fallback(feedback)
            
    except Exception as e:
        print(f"Erro geral no feedback: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Erro interno: {str(e)}"}
        )

async def submit_feedback_mongodb_fallback(feedback: FeedbackRequest):
    """Fallback para salvar feedback no MongoDB se o DynamoDB não estiver disponível"""
    try:
        client = conecta_mongodb()
        db = client["llm"]
        collection = db["chat_history"]
        
        # Converte string para ObjectId se necessário
        try:
            if ObjectId.is_valid(feedback.message_id):
                message_id = ObjectId(feedback.message_id)
            else:
                # Se não for um ObjectId válido, pode ser um índice numérico
                conversas = list(collection.find().sort("data", -1))
                if feedback.message_id.isdigit():
                    index = int(feedback.message_id) - 1
                    if 0 <= index < len(conversas):
                        message_id = conversas[index]["_id"]
                    else:
                        return JSONResponse(
                            status_code=400,
                            content={"success": False, "message": "ID da mensagem inválido"}
                        )
                else:
                    return JSONResponse(
                        status_code=400,
                        content={"success": False, "message": "Formato de ID inválido"}
                    )
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": f"Erro ao processar ID: {str(e)}"}
            )
        
        # Prepara os dados de feedback
        feedback_data = {
            "feedback_type": feedback.feedback_type,
            "comment": feedback.comment.strip() if feedback.comment else "",
            "feedback_date": datetime.now()
        }
        
        # Atualiza o documento com o feedback
        result = collection.update_one(
            {"_id": message_id},
            {"$set": feedback_data}
        )
        
        if result.matched_count > 0:
            feedback_text = "positivo" if feedback.feedback_type == 0 else "negativo"
            print(f"Feedback {feedback_text} salvo no MongoDB (fallback) para mensagem {message_id}")
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True, 
                    "message": "Feedback salvo com sucesso (MongoDB)!",
                    "feedback_type": feedback_text,
                    "fallback": True
                }
            )
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "message": "Mensagem não encontrada"}
            )
            
    except Exception as e:
        print(f"Erro ao salvar feedback: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Erro interno: {str(e)}"}
        )

# ----------------------------
# Proxy para PDF Processor
# ----------------------------
@app.post("/upload-pdf")
async def upload_pdf_proxy(
    file: UploadFile = File(...),
    request: Request = None
):
    """Proxy para upload de PDF do processador avançado"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        # URL do backend
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/upload-pdf"
        
        # Prepara o arquivo para envio
        file_data = await file.read()
        files = {
            'file': (file.filename, file_data, 'application/pdf')
        }
        
        # Prepara dados do usuário para envio
        data = {
            'user_id': user.get('email', 'unknown@example.com')  # Usar email como user_id
        }
        
        # Envia para o backend
        response = requests.post(BACKEND_URL, files=files, data=data, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
            
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=408, detail="Timeout: Processamento demorou muito")
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Erro de conexão com o backend")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

# ----------------------------
# Proxy para listar PDFs disponíveis
# ----------------------------
@app.get("/available-pdfs") 
async def available_pdfs_proxy(request: Request):
    """Proxy para listar PDFs disponíveis"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/available-pdfs"
        
        # Parâmetros para o backend
        params = {
            'user_id': user.get('email', 'unknown@example.com')  # Usar email como user_id
        }
        
        response = requests.get(BACKEND_URL, params=params, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            # Retorna o resultado diretamente, pois já está no formato esperado
            return result
        else:
            return {
                "available_files": [],
                "total_pdfs": 0,
                "error": response.text
            }
            
    except Exception as e:
        return {
            "available_files": [],
            "total_pdfs": 0,
            "error": str(e)
        }

@app.get("/upload-status/{task_id}")
async def get_upload_status_proxy(task_id: str, request: Request):
    """Proxy para verificar status do processamento"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/upload-status/{task_id}"
        response = requests.get(BACKEND_URL, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "task_id": task_id,
                "status": "error",
                "progress": 0,
                "message": "Erro ao verificar status",
                "error": response.text
            }
            
    except Exception as e:
        return {
            "task_id": task_id,
            "status": "error",
            "progress": 0,
            "message": "Erro de conexão",
            "error": str(e)
        }

# ----------------------------
# Função para buscar conversas do DynamoDB via backend
# ----------------------------
def get_chat_history_from_dynamodb(user_id: str, limit: int = 10):
    """Busca histórico de chat do DynamoDB via backend"""
    try:
        # Validar se user_id foi fornecido
        if not user_id:
            print("DEBUG DynamoDB: user_id não fornecido")
            return []
        
        # URL do backend para histórico de chat
        BACKEND_URL = f"{BACKEND_SERVICE_URL}/chat-history"
        
        # Parâmetros para a requisição
        params = {
            "limit": limit,
            "user_id": user_id
        }
        
        print(f"DEBUG DynamoDB: Buscando histórico de chat para usuário {user_id}")
        print(f"DEBUG DynamoDB: URL: {BACKEND_URL}")
        print(f"DEBUG DynamoDB: Params: {params}")
        
        # Fazer requisição para o backend
        response = requests.get(BACKEND_URL, params=params, timeout=10)
        
        print(f"DEBUG DynamoDB: Status da resposta: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            chats = result.get("chats", [])
            
            print(f"DEBUG DynamoDB: Encontrados {len(chats)} chats no DynamoDB")
            
            # Converter formato para compatibilidade com template
            conversas_formatadas = []
            for chat in chats:
                conversa_formatada = {
                    "pergunta": chat.get("pergunta", ""),
                    "resposta": chat.get("resposta", ""),
                    "data": chat.get("timestamp", datetime.now().isoformat()),
                    "_id": chat.get("chat_id", ""),
                    "user_id": chat.get("user_id", user_id),
                    "pdf_name": chat.get("pdf_name", ""),
                    "metadata": chat.get("metadata", {}),
                    "source": "dynamodb",  # Marcador para identificar origem
                    "feedback_type": chat.get("feedback_type", None),
                    "comment": chat.get("comment", "")
                }
                
                # Converter timestamp para datetime se necessário
                if isinstance(conversa_formatada["data"], str):
                    try:
                        conversa_formatada["data"] = datetime.fromisoformat(conversa_formatada["data"].replace('Z', '+00:00'))
                    except:
                        conversa_formatada["data"] = datetime.now()
                
                conversas_formatadas.append(conversa_formatada)
            
            print(f"DEBUG DynamoDB: Convertidas {len(conversas_formatadas)} conversas para formato do template")
            print(f"DEBUG DynamoDB: Conversas formatadas: {conversas_formatadas[:3]}...")  # Exibir apenas as 3 primeiras para debug
            return conversas_formatadas
            
        else:
            print(f"DEBUG DynamoDB: Erro ao buscar histórico: Status {response.status_code}, Resposta: {response.text}")
            return []
            
    except requests.exceptions.Timeout:
        print("DEBUG DynamoDB: Timeout na requisição")
        return []
    except requests.exceptions.ConnectionError:
        print("DEBUG DynamoDB: Erro de conexão com backend")
        return []
    except Exception as e:
        print(f"DEBUG DynamoDB: Erro inesperado ao buscar histórico: {str(e)}")
        return []

# ----------------------------
# Endpoint de teste para histórico do DynamoDB
# ----------------------------
@app.get("/test-dynamodb-history")
async def test_dynamodb_history(request: Request):
    """Endpoint para testar busca do histórico do DynamoDB"""
    # Verifica se o usuário está autenticado
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        user_id = user.get("id", "user_default_001")
        user_email = user.get("email", "unknown@example.com")
        user_name = user.get("name", "Usuário")
        print(f"DEBUG TEST: Testando histórico DynamoDB para usuário: {user_name} (Email: {user_email}, Google ID: {user_id})")
        
        # Buscar histórico do DynamoDB - tentar com email primeiro, depois com Google ID
        dynamodb_history = get_chat_history_from_dynamodb(user_email, limit=10)
        
        if not dynamodb_history:
            print(f"DEBUG TEST: Tentando buscar com Google ID: {user_id}")
            dynamodb_history = get_chat_history_from_dynamodb(user_id, limit=10)
        
        # Histórico local para comparação
        local_history = chat_history
        
        return {
            "user_id": user_id,
            "user_email": user_email,
            "user_name": user.get("name", "N/A"),
            "dynamodb_history": {
                "count": len(dynamodb_history),
                "conversations": dynamodb_history
            },
            "local_history": {
                "count": len(local_history),
                "conversations": local_history
            },
            "combined_total": len(dynamodb_history) + len(local_history),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "user_id": user.get("id", "unknown"),
            "timestamp": datetime.now().isoformat()
        }

