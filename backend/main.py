from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Form, BackgroundTasks, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging
import os
import time
import uuid
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import tempfile
from concurrent.futures import ThreadPoolExecutor

from services.dynamodb_service import DynamoDBService
from services.chromadb_client import ChromaDBService
from services.chat_service import ChatService
from services.db_service import DBService
from services.pdf_processing_service import PDFProcessingService
from services.s3_pdf_processor import S3PDFProcessor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI(
    title="ChatHib Backend",
    description="Backend com suporte a Chat com RAG, DynamoDB, ChromaDB e Geração de tabelas delta",
    version="2.1.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://chromadb-service:8001", 
        "http://127.0.0.1:8001",
        "http://frontend-service:8080",
        "http://127.0.0.1:8080"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]  
)

# Inicializar serviços
dynamodb_service = DynamoDBService()
chromadb_service = ChromaDBService()

use_bedrock = os.getenv("USE_BEDROCK", "true").lower() == "true"
chat_service = ChatService(use_bedrock=use_bedrock)

db_service = DBService()
pdf_processing_service = PDFProcessingService()

try:
    s3_processor = S3PDFProcessor(use_bedrock=use_bedrock)
    logger.info(f"S3PDFProcessor inicializado com sucesso (Bedrock: {use_bedrock})")
except Exception as e:
    logger.error(f"Erro ao inicializar S3PDFProcessor: {e}")
    s3_processor = None

# Cache para armazenar status de processamento
processing_status = {}

# Thread pool para processamento assíncrono
executor = ThreadPoolExecutor(max_workers=2)

class ProcessingStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"

class ChatRequest(BaseModel):
    message: str
    pdf_name: Optional[str] = None
    user_id: Optional[str] = None
    use_context: bool = True
    max_context_chunks: int = 5

class FeedbackRequest(BaseModel):
    message_id: str
    feedback_type: int  # 0 = positivo, 1 = negativo
    comment: Optional[str] = None
    user_id: Optional[str] = None

class QueryRequest(BaseModel):
    question: str
    pdf_name: str
    user_id: Optional[str] = None
    top_k: int = 5

class UserRequest(BaseModel):
    name: str
    email: str
    additional_info: Optional[Dict[str, Any]] = None

class PDFUploadResponse(BaseModel):
    success: bool
    message: str
    pdf_id: Optional[str] = None
    pdf_name: str
    chunks_created: int
    processing_time: str
    task_id: Optional[str] = None


def update_processing_status(task_id: str, status: str, progress: int = 0, message: str = "", result: dict = None):
    """Atualiza o status de processamento"""
    processing_status[task_id] = {
        "status": status,
        "progress": progress,
        "message": message,
        "result": result,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Log detalhado baseado no status
    if status == ProcessingStatus.COMPLETED:
        logger.info(f"PROCESSAMENTO CONCLUÍDO para {task_id}: {message}")
    elif status == ProcessingStatus.ERROR:
        logger.error(f"ERRO no processamento {task_id}: {message}")
    elif status == ProcessingStatus.PROCESSING:
        logger.info(f"Processando {task_id}: {message}")
    else:
        logger.info(f"Status atualizado para {task_id}: {status} ({progress}%) - {message}")

    # Log adicional para debug
    logger.debug(f"Task {task_id} - Status: {status}, Progress: {progress}%, Total tasks: {len(processing_status)}")

def _format_processing_time(seconds: float) -> str:
    """Formata tempo de processamento em formato legível"""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}min"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"

def process_pdf_sync(file_content: bytes, filename: str, user_id: str, task_id: str):
    """Processa PDF em background"""
    start_time = time.time()
    
    try:
        update_processing_status(task_id, ProcessingStatus.PROCESSING, 20, f"Processando {filename}...")
        
        # Verificar se o S3 processor está disponível
        if s3_processor is None:
            logger.error("S3PDFProcessor não foi inicializado corretamente")
            update_processing_status(task_id, ProcessingStatus.ERROR, 0, "S3PDFProcessor não disponível")
            return

            
        # Verificar conexão S3
        s3_available = s3_processor.test_s3_connection()
        if not s3_available:
            logger.warning("S3 não disponível")
            
        # Salvar arquivo temporário para processamento S3
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file.write(file_content)
            tmp_file_path = tmp_file.name
        
        logger.info(f"Processando PDF com S3: {tmp_file_path}")
        
        # Processar PDF com extração de tabelas e salvamento no S3
        update_processing_status(task_id, ProcessingStatus.PROCESSING, 50, f"Extraindo tabelas de {filename}...")
        s3_result = s3_processor.process_pdf_with_table_extraction(tmp_file_path, original_filename=filename)
        
        # Limpar arquivo temporário
        os.unlink(tmp_file_path)
        
        if 'error' in s3_result:
            error_message = s3_result['error']
            error_type = s3_result.get('error_type', 'unknown')
            solution = s3_result.get('solution', '')
            
            # Mensagem detalhada baseada no tipo de erro
            if error_type == 'dependency':
                detailed_message = f"Dependência faltando: {error_message}"
                if solution:
                    detailed_message += f" |  Solução: {solution}"
            elif error_type == 'encryption':
                detailed_message = f"PDF criptografado: {error_message}"
                if solution:
                    detailed_message += f" |  Solução: {solution}"
            else:
                detailed_message = f"Erro no processamento: {error_message}"

            logger.error(f"Erro no processamento S3: {detailed_message}")
            update_processing_status(task_id, ProcessingStatus.ERROR, 0, detailed_message)
            return
        
        # Processar (ChromaDB + DynamoDB)
        update_processing_status(task_id, ProcessingStatus.PROCESSING, 70, f"Indexando no ChromaDB...")
        traditional_result = pdf_processing_service.process_uploaded_pdf(
            file_content=file_content,
            filename=filename,
            user_id=user_id
        )
        
        # Calcular tempo de processamento total
        total_processing_time = time.time() - start_time
        
        # Combinar resultados
        combined_result = {
            'success': True,
            'pdf_name': filename,
            'pdf_id': traditional_result.get('pdf_id') if traditional_result.get('success') else None,
            's3_processing': s3_result,
            'traditional_processing': traditional_result,
            'tables_extracted': s3_result.get('tables_extracted', []),
            's3_csv_files': s3_result.get('s3_csv_files', {}),
            's3_delta_files': s3_result.get('s3_delta_files', {}),
            'chunks_created': traditional_result.get('extraction_stats', {}).get('chunks_created', 0),
            'processing_time': datetime.utcnow().isoformat(),
            'processing_time_seconds': total_processing_time,
            'processing_time_formatted': _format_processing_time(total_processing_time)
        }
        
        # Logs
        if s3_result.get('tables_extracted'):
            for table_name in s3_result['tables_extracted']:
                table_count = len(s3_result['tables'][table_name])
                logger.info(f"Tabela {table_name}: {table_count} linhas extraídas")
        
        if s3_result.get('s3_delta_files'):
            logger.info(f"Gerando tabelas deltas para {filename}...")
            for delta_name, delta_path in s3_result['s3_delta_files'].items():
                logger.info(f"{delta_name}: salvo -> {delta_path}")
        
        # Log do tempo de processamento
        logger.info(f"Tempo total de processamento para {filename}: {combined_result['processing_time_formatted']} ({total_processing_time:.2f}s)")
        
        # Gerar mensagem detalhada de conclusão
        tables_count = len(s3_result.get('tables_extracted', []))
        csv_count = len(s3_result.get('s3_csv_files', {}))
        delta_count = len(s3_result.get('s3_delta_files', {}))
        
        success_message = f"Processamento concluído em {combined_result['processing_time_formatted']}! {tables_count} tabelas extraídas, {csv_count} CSVs e {delta_count} deltas salvos no S3."
        if s3_result.get('tables_extracted'):
            success_message += f" | Tabelas: {', '.join(s3_result['tables_extracted'])}"
        success_message += f" | CSV: {csv_count} arquivos | Delta: {delta_count} arquivos | Tempo: {combined_result['processing_time_formatted']}"
        
        update_processing_status(task_id, ProcessingStatus.COMPLETED, 100, success_message, combined_result)
            
    except Exception as e:
        # Limpar arquivo temporário se houver erro
        if 'tmp_file_path' in locals():
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        logger.error(f"Erro no processamento em background: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        update_processing_status(task_id, ProcessingStatus.ERROR, 0, f"Erro interno: {str(e)}")

# Modelos Pydantic para requests

# Dependency para obter user_id (simulado)
def get_current_user_id(request: Request = None):
    """Obtém o email do usuário atual da sessão"""
    try:
        if not request:
            return "user_default_001"  # Fallback para compatibilidade
        
        # Verifica se há token nos cookies
        access_token = request.cookies.get("access_token")
        if not access_token:
            return "user_default_001"
        
        # Verifica se há informações do usuário nos cookies
        user_info = request.cookies.get("user_info")
        if user_info:
            user_data = json.loads(user_info)
            # Retorna o email do usuário
            email = (
                user_data.get("email") or
                user_data.get("mail") or 
                user_data.get("userPrincipalName") or 
                user_data.get("preferredUsername") or
                "user_default_001"
            )
            return email
        
        return "user_default_001"
    except Exception:
        return "user_default_001"

def get_user_id_dependency(request: Request):
    """Wrapper para usar como dependência do FastAPI"""
    return get_current_user_id(request)

# Endpoints principais

@app.get("/health")
async def health_check():
    """Health check para Kubernetes"""
    try:
        # Health check básico - só verifica se a aplicação está rodando
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "backend": "running"
            },
            "version": "0.0.7"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy", 
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/health/detailed")
async def detailed_health_check():
    """Health check detalhado para debug - não usa em probes do Kubernetes"""
    try:
        # Verifica serviços externos com timeout
        chromadb_health = False
        try:
            chromadb_health = chromadb_service.client.health_check()
        except Exception as e:
            logger.warning(f"ChromaDB health check failed: {e}")
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "dynamodb": "connected",
                "chromadb": "connected" if chromadb_health else "disconnected",
                "backend": "running"
            },
            "version": "0.0.7"
        }
    except Exception as e:
        logger.error(f"Detailed health check failed: {e}")
        return {
            "status": "unhealthy", 
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/health/processing")
async def health_check_processing():
    """Health check que considera processamento ativo"""
    try:
        # Verifica se há processamento ativo
        active_tasks = len([task for task in processing_status.values() 
                          if task['status'] == ProcessingStatus.PROCESSING])
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "active_tasks": active_tasks,
            "processing_active": active_tasks > 0,
            "services": {
                "backend": "running"
            },
            "version": "2.0.0"
        }
    except Exception as e:
        logger.error(f"Processing health check failed: {e}")
        return {
            "status": "unhealthy", 
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.post("/upload-pdf", response_model=PDFUploadResponse)
async def upload_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_id: str = Form(default=None)
):
    """Upload e processamento assíncrono de PDF"""
    try:
        # Se user_id não foi passado no Form, usa o padrão da dependência
        if not user_id:
            user_id = Depends(get_user_id_dependency)
            print(f"USUARIO DA get_user_id_dependency: {user_id}")
        
        logger.info(f"Iniciando upload de PDF: {file.filename} para usuário: {user_id}")
        
        if not file.filename.lower().endswith('.pdf'):
            logger.warning(f"Tipo de arquivo inválido: {file.filename}")
            raise HTTPException(status_code=400, detail="Apenas arquivos PDF são permitidos")

        # Ler conteúdo do arquivo
        file_content = await file.read()
        logger.info(f"Arquivo lido: {len(file_content)} bytes")
        
        # Gerar ID de tarefa único para rastrear o processamento
        task_id = str(uuid.uuid4())
        
        # Inicializar status
        update_processing_status(task_id, ProcessingStatus.PENDING, 10, f"Iniciando processamento de {file.filename}...")
        
        # Iniciar processamento em background usando BackgroundTasks
        background_tasks.add_task(process_pdf_sync, file_content, file.filename, user_id, task_id)
        
        logger.info(f"Processamento agendado para background - Task ID: {task_id}")
        
        # Retornar resposta imediata com o ID da tarefa
        return PDFUploadResponse(
            success=True,
            message=f"PDF '{file.filename}' recebido e está sendo processado com extração de tabelas S3 em segundo plano.",
            pdf_id=None,
            pdf_name=file.filename,
            chunks_created=0,
            processing_time="0s",
            task_id=task_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no upload de PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@app.post("/chat")
async def chat_with_documents(
    request: ChatRequest,
    user_id: str = Depends(get_user_id_dependency)
):
    """Endpoint para chat com documentos usando RAG"""
    try:
        # Usar user_id do request se fornecido, senão usar o padrão
        actual_user_id = request.user_id or user_id
        
        if request.pdf_name:
            # Chat específico com um PDF usando ChromaDB
            print(f"DEBUG: Chat com PDF específico: {request.pdf_name}")
            
            # Buscar documentos similares no ChromaDB
            similar_docs = chromadb_service.search_similar_content(
                query=request.message,
                pdf_name=request.pdf_name,
                user_id=actual_user_id,
                max_results=request.max_context_chunks
            )
            
            print(f"DEBUG: Encontrados {len(similar_docs)} documentos similares no ChromaDB")
            
            if not similar_docs:
                return {
                    "message": request.message,
                    "response": f"Não encontrei informações relevantes no PDF '{request.pdf_name}' para responder sua pergunta. Você poderia reformular ou fazer uma pergunta mais específica?",
                    "context_used": False,
                    "sources": [],
                    "pdf_name": request.pdf_name,
                    "error": "Nenhum contexto relevante encontrado"
                }
            
            # Preparar contexto para o modelo
            context_text = " ".join([doc.get("text", "") for doc in similar_docs])
            sources = []
            
            for doc in similar_docs:
                metadata = doc.get("metadata", {})
                sources.append({
                    "text": doc.get("text", "")[:200] + "...",
                    "pdf_name": metadata.get("pdf_name", request.pdf_name),
                    "chunk_index": metadata.get("chunk_index", 0),
                    "similarity_score": doc.get("score", 0)
                })
            
            # Usar o chat service para gerar resposta
            result = chat_service.ask_question(
                question=request.message,
                pdf_name=request.pdf_name,
                user_id=actual_user_id,
                context_override=context_text  # Passa o contexto do ChromaDB
            )
            
            return {
                "message": request.message,
                "response": result.get("answer", "Não foi possível gerar uma resposta."),
                "context_used": True,
                "sources": sources,
                "metadata": {
                    "chunks_used": len(similar_docs),
                    "pdf_name": request.pdf_name,
                    "user_id": actual_user_id
                },
                "pdf_name": request.pdf_name
            }
        else:
            # Chat geral - buscar em todos os PDFs
            print(f"DEBUG: Chat geral - buscando em todos os PDFs")

            # Buscar documentos similares em todos os PDFs
            similar_docs = chromadb_service.search_similar_content(
                query=request.message,
                pdf_name=None,  # Busca em todos os PDFs
                user_id=actual_user_id,
                max_results=request.max_context_chunks
            )
            
            if not similar_docs:
                return {
                    "message": request.message,
                    "response": "Não encontrei informações relevantes nos seus documentos para responder essa pergunta. Você poderia especificar um PDF específico ou fazer uma pergunta mais detalhada?",
                    "context_used": False,
                    "sources": [],
                    "metadata": {"user_id": actual_user_id},
                    "suggestion": "Tente especificar um PDF usando o campo 'pdf_name' ou reformule sua pergunta."
                }
            
            # Preparar contexto
            context_text = " ".join([doc.get("text", "") for doc in similar_docs])
            sources = []
            
            for doc in similar_docs:
                metadata = doc.get("metadata", {})
                sources.append({
                    "text": doc.get("text", "")[:200] + "...",
                    "pdf_name": metadata.get("pdf_name", "Unknown"),
                    "chunk_index": metadata.get("chunk_index", 0),
                    "similarity_score": doc.get("score", 0)
                })
            
            # Gerar resposta usando chat service
            result = chat_service.ask_question_general(
                question=request.message,
                user_id=actual_user_id,
                context_override=context_text
            )
            
            # Garantir que sempre há uma resposta, mesmo se o LLM falhar
            if not result.get("answer") or result.get("answer") == "Não foi possível gerar uma resposta.":
                # Fallback: criar resposta baseada no contexto encontrado
                fallback_response = f"Encontrei informações relevantes sobre '{request.message}' nos documentos:\n\n"
                for i, doc in enumerate(similar_docs[:2], 1):
                    fallback_response += f"{i}. {doc.get('text', '')[:300]}...\n\n"
                fallback_response += "Baseado nessas informações dos documentos indexados."
                result["answer"] = fallback_response
            
            return {
                "message": request.message,
                "response": result.get("answer", "Não foi possível gerar uma resposta."),
                "context_used": True,
                "sources": sources,
                "metadata": {
                    "chunks_used": len(similar_docs),
                    "pdfs_found": len(set(s["pdf_name"] for s in sources)),
                    "user_id": actual_user_id
                }
            }
        
    except Exception as e:
        logger.error(f"Erro no chat: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro no chat: {str(e)}")

@app.post("/query")
async def query_documents(
    request: QueryRequest,
    user_id: str = Depends(get_user_id_dependency)
):
    """Query semântica nos documentos"""
    try:
        actual_user_id = request.user_id or user_id
        
        # Buscar documentos similares no ChromaDB
        similar_docs = chromadb_service.search_similar_content(
            query=request.question,
            pdf_name=request.pdf_name,
            user_id=actual_user_id,
            max_results=request.top_k
        )
        
        if not similar_docs:
            return {
                "question": request.question,
                "pdf_name": request.pdf_name,
                "context": [],
                "total_docs_found": 0,
                "message": f"Nenhum conteúdo encontrado para o PDF '{request.pdf_name}'"
            }
        
        # Formatar resultados
        context_chunks = []
        for doc in similar_docs:
            context_chunks.append({
                "text": doc.get("text", ""),
                "metadata": doc.get("metadata", {}),
                "similarity_score": doc.get("score", 0),
                "chunk_id": doc.get("id", "")
            })
        
        return {
            "question": request.question,
            "pdf_name": request.pdf_name,
            "context": context_chunks,
            "total_docs_found": len(context_chunks),
            "query_metadata": {
                "user_id": actual_user_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Erro na query: {e}")
        raise HTTPException(status_code=500, detail=f"Erro na query: {str(e)}")

@app.get("/pdfs")
async def list_user_pdfs(user_id: str = None):
    """Lista PDFs do usuário"""
    try:
        # Se não fornecido, usa o usuário padrão
        if not user_id:
            user_id: str = Depends(get_user_id_dependency)
            
        pdfs = db_service.list_pdfs()
        
        return {
            "user_id": user_id,
            "pdfs": pdfs,
            "total_pdfs": len(pdfs),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar PDFs: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao listar PDFs: {str(e)}")
    
@app.get("/pdfs_user")
async def list_user_pdfs(user_id: str = None):
    """Lista PDFs do usuário"""
    try:
        # Se não fornecido, usa o usuário padrão
        if not user_id:
            user_id: str = Depends(get_user_id_dependency)
            
        pdfs = db_service.list_user_pdfs(user_id)
        
        return {
            "user_id": user_id,
            "pdfs": pdfs,
            "total_pdfs": len(pdfs),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar PDFs: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao listar PDFs: {str(e)}")

@app.get("/pdfs/{pdf_name}/status")
async def get_pdf_status(
    pdf_name: str,
    user_id: str = Depends(get_user_id_dependency)
):
    """Obtém status de processamento de um PDF"""
    try:
        status = pdf_processing_service.get_pdf_processing_status(pdf_name, user_id)
        return status
        
    except Exception as e:
        logger.error(f"Erro ao obter status do PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter status: {str(e)}")

@app.delete("/pdfs/{pdf_name}")
async def delete_pdf(
    pdf_name: str,
    user_id: str = Depends(get_user_id_dependency)
):
    """Remove um PDF e todos os seus dados"""
    try:
        result = pdf_processing_service.delete_pdf_data(pdf_name, user_id)
        
        if not result['success']:
            raise HTTPException(status_code=500, detail=result.get('error', 'Erro desconhecido'))
        
        return {
            "message": f"PDF '{pdf_name}' removido com sucesso",
            "pdf_name": pdf_name,
            "details": result['details']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao deletar PDF: {str(e)}")

@app.get("/chat-history")
async def get_chat_history(
    user_id: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100)
):
    """Obtém histórico de chat do usuário - busca apenas por user_id e timestamp"""
    try:
        # Se user_id não for fornecido, usar o padrão
        if not user_id:
            user_id: str = Depends(get_user_id_dependency)
        
        logger.info(f"Backend: Buscando chat history para user_id: {user_id}, limit: {limit}")
        logger.info(f"Backend: DynamoDB disponível: {dynamodb_service.is_available()}")
        
        # Buscar diretamente no DynamoDB apenas por user_id e timestamp
        chats = dynamodb_service.get_chat_history(user_id, limit)
        
        logger.info(f"Backend: Encontrados {len(chats)} chats no DynamoDB para user_id: {user_id}")
        
        # Formatar dados para compatibilidade com frontend
        formatted_chats = []
        for chat in chats:
            formatted_chat = {
                "chat_id": chat.get("chat_id", ""),
                "_id": chat.get("chat_id", ""),  # Compatibilidade
                "user_id": chat.get("user_id", ""),
                "pergunta": chat.get("question", ""),
                "resposta": chat.get("answer", ""),
                "pdf_name": chat.get("pdf_name", ""),
                "data": chat.get("timestamp", ""),
                "timestamp": chat.get("timestamp", ""),
                "feedback_type": chat.get("feedback_type"),
                "comment": chat.get("feedback_comment", ""),
                "metadata": chat.get("metadata", {})
            }
            formatted_chats.append(formatted_chat)
        
        return {
            "user_id": user_id,
            "chats": formatted_chats,
            "total_chats": len(formatted_chats),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Backend: Erro ao obter histórico de chat: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter histórico: {str(e)}")

@app.get("/debug/chat-history")
async def debug_chat_history():
    """Debug endpoint para verificar se há chats salvos no DynamoDB"""
    try:
        logger.info("DEBUG: Verificando status do DynamoDB")
        
        # Verificar se DynamoDB está disponível
        is_available = dynamodb_service.is_available()
        logger.info(f"DEBUG: DynamoDB disponível: {is_available}")
        
        if not is_available:
            return {
                "status": "error",
                "message": "DynamoDB não está disponível",
                "available": False
            }
        
        # Tentar listar todos os chats (scan limitado)
        try:
            table = dynamodb_service.dynamodb.Table(dynamodb_service.tables['chat_history'])
            response = table.scan(Limit=10)
            items = response.get('Items', [])
            
            logger.info(f"DEBUG: Encontrados {len(items)} chats na tabela")
            
            return {
                "status": "success",
                "dynamodb_available": True,
                "total_chats_found": len(items),
                "sample_chats": items[:3],  # Mostrar apenas os primeiros 3
                "table_name": dynamodb_service.tables['chat_history']
            }
            
        except Exception as scan_error:
            logger.error(f"DEBUG: Erro no scan da tabela: {scan_error}")
            return {
                "status": "error",
                "message": f"Erro ao acessar tabela: {str(scan_error)}",
                "dynamodb_available": True
            }
        
    except Exception as e:
        logger.error(f"DEBUG: Erro geral: {e}")
        return {
            "status": "error",
            "message": f"Erro geral: {str(e)}"
        }

@app.post("/users", response_model=Dict[str, Any])
async def create_user(user_data: UserRequest):
    """Cria um novo usuário ou atualiza existente"""
    try:
        user_dict = {
            "name": user_data.name,
            "email": user_data.email
        }
        
        if user_data.additional_info:
            user_dict["additional_info"] = user_data.additional_info
        
        user_id = dynamodb_service.create_user(user_dict)
        
        # Busca o usuário completo para retornar
        user_info = dynamodb_service.get_user(user_id)
        
        return {
            "message": "Usuário processado com sucesso",
            "user": user_info or {"user_id": user_id}
        }
        
    except Exception as e:
        logger.error(f"Erro ao processar usuário: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar usuário: {str(e)}")

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    """Obtém dados de um usuário"""
    try:
        user = dynamodb_service.get_user(user_id)
        
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")
        
        return {
            "user": user
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao obter usuário: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter usuário: {str(e)}")

@app.get("/stats")
async def get_system_stats(user_id: str = Depends(get_user_id_dependency)):
    """Obtém estatísticas do sistema"""
    try:
        stats = db_service.get_database_stats(user_id)
        
        return {
            "stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter estatísticas: {str(e)}")

@app.post("/reprocess-pdf/{pdf_name}")
async def reprocess_pdf(
    pdf_name: str,
    user_id: str = Depends(get_user_id_dependency)
):
    """Reprocessa um PDF existente"""
    try:
        result = pdf_processing_service.reprocess_pdf(pdf_name, user_id)
        
        if not result['success']:
            raise HTTPException(status_code=500, detail=result.get('error', 'Erro no reprocessamento'))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao reprocessar PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao reprocessar: {str(e)}")

# Endpoint de compatibilidade para o frontend
@app.get("/available-pdfs")
async def available_pdfs(user_id: str = None):
    """Endpoint de compatibilidade para listar PDFs disponíveis"""
    try:
        # Se não fornecido, usa o usuário padrão
        if not user_id:
            user_id: str = Depends(get_user_id_dependency)
            
        pdfs = db_service.list_user_pdfs(user_id)
        
        # Adapta formato para compatibilidade com frontend
        formatted_pdfs = []
        for pdf in pdfs:
            formatted_pdfs.append({
                "filename": pdf.get("filename", pdf.get("name", "Unknown")),
                "upload_date": pdf.get("upload_date", datetime.utcnow().isoformat()),
                "status": pdf.get("status", "processed"),
                "size": pdf.get("size", 0),
                "pages": pdf.get("pages", 0)
            })
        
        return {
            "available_files": formatted_pdfs,
            "total_pdfs": len(formatted_pdfs),
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar PDFs disponíveis: {e}")
        return {
            "available_files": [],
            "total_pdfs": 0,
            "error": str(e)
        }

## Endpoints de compatibilidade com a primeira versão
@app.get("/recent-chats")
async def get_recent_chats_legacy(user_id: str = Depends(get_user_id_dependency)):
    """Endpoint de compatibilidade para chats recentes"""
    try:
        chats = db_service.recent_chats(user_id, 10)
        return chats
        
    except Exception as e:
        logger.error(f"Erro nos chats recentes: {e}")
        return []

@app.post("/create-table-from-pdf")
async def create_table_from_pdf_legacy(
    request: Dict[str, str],
    user_id: str = Depends(get_user_id_dependency)
):
    """Endpoint de compatibilidade para criação de tabela"""
    try:
        pdf_name = request.get("pdf_name")
        if not pdf_name:
            raise HTTPException(status_code=400, detail="pdf_name é obrigatório")
        
        result = db_service.create_table_from_pdf(pdf_name, user_id)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao criar tabela: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar tabela: {str(e)}")

@app.get("/upload-status/{task_id}")
async def get_upload_status(task_id: str):
    """Verifica o status de processamento de upload por task_id"""
    try:
        if task_id in processing_status:
            status_data = processing_status[task_id]
            
            # Adicionar informações extras para debug
            status_data_with_debug = {
                **status_data,
                "task_id": task_id,
                "is_completed": status_data.get('status') == ProcessingStatus.COMPLETED,
                "is_error": status_data.get('status') == ProcessingStatus.ERROR,
                "is_processing": status_data.get('status') == ProcessingStatus.PROCESSING,
                "debug_info": {
                    "total_tasks_in_memory": len(processing_status),
                    "current_timestamp": datetime.utcnow().isoformat()
                }
            }
            
            return status_data_with_debug
        else:
            raise HTTPException(status_code=404, detail="Task ID não encontrado")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao verificar status: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao verificar status: {str(e)}")

@app.get("/test-chromadb")
async def test_chromadb():
    """Testa conectividade com ChromaDB"""
    try:
        # Testa health check
        health = chromadb_service.client.health_check()
        
        # Testa listar coleções (se o endpoint existir)
        collections_info = {"error": "Endpoint não disponível"}
        try:
            collections_info = chromadb_service.client.list_collections()
        except Exception as e:
            collections_info = {"error": str(e)}
        
        return {
            "chromadb_health": health,
            "collections": collections_info,
            "base_url": chromadb_service.client.base_url,
            "default_collection": chromadb_service.default_collection,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro no teste ChromaDB: {e}")
        return {
            "error": str(e),
            "chromadb_health": False,
            "base_url": chromadb_service.client.base_url,
            "timestamp": datetime.utcnow().isoformat()
        }

@app.post("/test-chat-simple")
async def test_chat_simple(request: dict):
    """Endpoint simples para testar chat com ChromaDB"""
    try:
        message = request.get("message", "")
        pdf_name = request.get("pdf_name", "")
        
        if not message:
            raise HTTPException(status_code=400, detail="Message é obrigatório")
        
        print(f"DEBUG: Testando chat - message: {message}, pdf_name: {pdf_name}")
        
        # Buscar diretamente no ChromaDB
        similar_docs = chromadb_service.search_similar_content(
            query=message,
            pdf_name=pdf_name if pdf_name else None,
            user_id="user_default_001",
            max_results=3
        )
        
        print(f"DEBUG: ChromaDB retornou {len(similar_docs)} documentos")
        
        if not similar_docs:
            return {
                "message": message,
                "response": "Não encontrei informações relevantes nos documentos.",
                "context_used": False,
                "sources": [],
                "debug": "Nenhum documento similar encontrado no ChromaDB"
            }
        
        # Preparar contexto simples
        context_texts = [doc.get("text", "") for doc in similar_docs]
        context = " ".join(context_texts)
        
        # Resposta simples sem LLM para teste
        simple_response = f"Encontrei {len(similar_docs)} trechos relevantes. Primeiro trecho: {context_texts[0][:200]}..."
        
        return {
            "message": message,
            "response": simple_response,
            "context_used": True,
            "sources": similar_docs,
            "debug": {
                "docs_found": len(similar_docs),
                "context_length": len(context),
                "pdf_name": pdf_name
            }
        }
        
    except Exception as e:
        logger.error(f"Erro no teste de chat: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "error": str(e),
            "message": request.get("message", ""),
            "debug": "Erro durante processamento"
        }

@app.get("/test-chromadb-data")
async def test_chromadb_data():
    """Testa se há dados no ChromaDB e como estão estruturados"""
    try:
        # Fazer uma query sem filtros para ver todos os dados
        result = chromadb_service.client.query_documents(
            collection_name="rag_documents",
            query_text="documento",  # Query genérica
            n_results=10,
            filter_metadata=None
        )
        
        # Testar também com filtro específico se houver dados
        result_with_filter = None
        if result.get("documents"):
            # Pegar metadados do primeiro documento para teste
            first_metadata = result.get("metadatas", [{}])[0] if result.get("metadatas") else {}
            if "pdf_name" in first_metadata:
                result_with_filter = chromadb_service.client.query_documents(
                    collection_name="rag_documents",
                    query_text="documento",
                    n_results=5,
                    filter_metadata={"pdf_name": first_metadata["pdf_name"]}
                )
        
        return {
            "collection_name": "rag_documents",
            "query_all_results": {
                "total_found": len(result.get("documents", [])),
                "documents": result.get("documents", [])[:3],  # Primeiros 3 apenas
                "metadatas": result.get("metadatas", [])[:3],
                "distances": result.get("distances", [])[:3],
                "ids": result.get("ids", [])[:3]
            },
            "query_with_filter": result_with_filter,
            "debug_info": {
                "chromadb_health": chromadb_service.client.health_check(),
                "default_collection": chromadb_service.default_collection
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro no teste ChromaDB data: {e}")
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.post("/test-global-search")
async def test_global_search(request: dict):
    """
    Testa busca global no ChromaDB sem filtros de usuário
    """
    try:
        message = request.get("message", "documento")
        print(f"DEBUG Global Search: Testando busca global com query: '{message}'")
        
        # Buscar GLOBALMENTE sem filtros de usuário
        similar_docs = chromadb_service.search_similar_content(
            query=message,
            pdf_name=None,  # Buscar em todos os PDFs
            user_id=None,   # Não filtrar por usuário
            max_results=10
        )
        
        print(f"DEBUG Global Search: Encontrados {len(similar_docs)} documentos")
        
        # Mostrar metadados para debug
        metadatas_summary = []
        for doc in similar_docs:
            metadata = doc.get("metadata", {})
            metadatas_summary.append({
                "pdf_name": metadata.get("pdf_name", "N/A"),
                "chunk_index": metadata.get("chunk_index", "N/A"),
                "has_user_id": "user_id" in metadata,
                "user_id_value": metadata.get("user_id", "N/A"),
                "text_preview": doc.get("text", "")[:100] + "..."
            })
        
        return {
            "query": message,
            "total_found": len(similar_docs),
            "documents_preview": metadatas_summary,
            "success": True,
            "global_search": True,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"DEBUG Global Search: Erro: {e}")
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }

@app.post("/upload-pdf-s3")
async def upload_pdf_s3(
    file: UploadFile = File(...),
    user_id: str = Form(default="user_default_001")
):
    """Upload PDF com extração de tabelas e salvamento no S3"""
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Apenas arquivos PDF são permitidos")

    try:
        # Verificar se S3 processor está disponível
        if s3_processor is None:
            raise HTTPException(status_code=503, detail="S3PDFProcessor não disponível")
            
        # Gera ID único para esta tarefa
        task_id = str(uuid.uuid4())
        
        # Salva arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            pdf_content = await file.read()
            tmp_file.write(pdf_content)
            tmp_file_path = tmp_file.name

        print(f"Processando PDF: {tmp_file_path}")
        
        # Processa PDF com extração de tabelas
        result = s3_processor.process_pdf_with_table_extraction(tmp_file_path, original_filename=file.filename)
        
        # Limpa arquivo temporário
        os.unlink(tmp_file_path)
        
        if 'error' in result:
            error_message = result['error']
            error_type = result.get('error_type', 'unknown')
            solution = result.get('solution', '')
            
            # Criar mensagem de erro mais informativa
            if error_type == 'dependency':
                detail_message = f"Dependência faltando: {error_message}"
                if solution:
                    detail_message += f" | Solução: {solution}"
                raise HTTPException(status_code=422, detail=detail_message)
            elif error_type == 'encryption':
                detail_message = f"PDF criptografado: {error_message}"
                if solution:
                    detail_message += f" | Solução: {solution}"
                raise HTTPException(status_code=422, detail=detail_message)
            else:
                raise HTTPException(status_code=500, detail=error_message)
        
        # Retorna resultado com informações do S3
        return {
            "task_id": task_id,
            "message": f"PDF processado com sucesso. Tabelas extraídas e salvas no S3.",
            "pdf_info": result["pdf_info"],
            "tables_extracted": result["tables_extracted"],
            "s3_csv_files": result["s3_csv_files"],
            "s3_delta_files": result["s3_delta_files"],
            "processing_date": result["processing_date"]
        }

    except Exception as e:
        # Limpa arquivo temporário se houver erro
        if 'tmp_file_path' in locals():
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Erro ao processar PDF: {str(e)}")

@app.get("/s3-status")
async def check_s3_status():
    """Verifica status da conexão com S3"""
    try:
        if s3_processor is None:
            return {
                "s3_available": False,
                "error": "S3PDFProcessor não inicializado",
                "timestamp": datetime.now().isoformat()
            }
            
        s3_available = s3_processor.test_s3_connection()
        
        return {
            "s3_available": s3_available,
            "bucket_name": s3_processor.bucket_name,
            "s3_folder": s3_processor.s3_folder,
            "google_ai_available": s3_processor.model is not None,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "s3_available": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.post("/process-pdf-tables")
async def process_pdf_tables(
    file: UploadFile = File(...),
    target_tables: str = Form(default="investimento_financeiro,valores_contrato,produtos_servicos,cronograma_pagamentos,partes_contrato"),
    user_id: str = Form(default="user_default_001")
):
    """Processa PDF e extrai tabelas específicas (compatível com logs antigos)"""
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Apenas arquivos PDF são permitidos")

    try:
        # Verificar se S3 processor está disponível
        if s3_processor is None:
            raise HTTPException(status_code=503, detail="S3PDFProcessor não disponível")
            
        # Converte string de tabelas para lista
        target_tables_list = [table.strip() for table in target_tables.split(',')]
        
        # Salva arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            pdf_content = await file.read()
            tmp_file.write(pdf_content)
            tmp_file_path = tmp_file.name

        print(f"Processando PDF: {tmp_file_path}")
        
        # Processa PDF com extração de tabelas
        result = s3_processor.process_pdf_with_table_extraction(tmp_file_path, target_tables_list, original_filename=file.filename)
        
        # Limpa arquivo temporário
        os.unlink(tmp_file_path)
        
        if 'error' in result:
            error_message = result['error']
            error_type = result.get('error_type', 'unknown')
            solution = result.get('solution', '')
            
            # Criar mensagem de erro mais informativa
            if error_type == 'dependency':
                detail_message = f"Dependência faltando: {error_message}"
                if solution:
                    detail_message += f" | Solução: {solution}"
                raise HTTPException(status_code=422, detail=detail_message)
            elif error_type == 'encryption':
                detail_message = f"PDF criptografado: {error_message}"
                if solution:
                    detail_message += f" | Solução: {solution}"
                raise HTTPException(status_code=422, detail=detail_message)
            else:
                raise HTTPException(status_code=500, detail=error_message)
        
        # Gera logs compatíveis com o sistema antigo
        for table_name in result['tables_extracted']:
            table_count = len(result['tables'][table_name])
            print(f"Tabela {table_name}: {table_count} linhas extraídas")
        
        # Gera tabelas deltas (compatível com logs antigos)
        if result['s3_delta_files']:
            print(f"Gerando tabelas deltas para {file.filename}...")
            print(f"Gerando tabelas deltas para {file.filename}...")
            
            for delta_name, delta_path in result['s3_delta_files'].items():
                print(f"{delta_name}: salvo -> {delta_path}")
        
        return {
            "message": f"PDF processado com sucesso. {len(result['tables_extracted'])} tabelas extraídas.",
            "pdf_info": result["pdf_info"],
            "tables_extracted": result["tables_extracted"],
            "tables_data": {name: df.to_dict('records') for name, df in result['tables'].items()},
            "s3_csv_files": result["s3_csv_files"],
            "s3_delta_files": result["s3_delta_files"],
            "processing_date": result["processing_date"]
        }

    except Exception as e:
        # Limpa arquivo temporário se houver erro
        if 'tmp_file_path' in locals():
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Erro ao processar PDF: {str(e)}")

@app.get("/s3-files")
async def list_s3_files(folder: str = Query(default="csv")):
    """Lista arquivos salvos no S3"""
    try:
        if s3_processor is None:
            raise HTTPException(status_code=503, detail="S3PDFProcessor não disponível")
            
        if not s3_processor.s3_client:
            raise HTTPException(status_code=503, detail="S3 não configurado")
        
        # Lista objetos no S3
        response = s3_processor.s3_client.list_objects_v2(
            Bucket=s3_processor.bucket_name,
            Prefix=f"{s3_processor.s3_folder}/{folder}/"
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    "key": obj['Key'],
                    "size": obj['Size'],
                    "last_modified": obj['LastModified'].isoformat(),
                    "storage_class": obj.get('StorageClass', 'STANDARD'),
                    "s3_url": f"s3://{s3_processor.bucket_name}/{obj['Key']}"
                })
        
        return {
            "bucket": s3_processor.bucket_name,
            "folder": f"{s3_processor.s3_folder}/{folder}",
            "files": files,
            "total_files": len(files),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar arquivos S3: {str(e)}")

@app.post("/clear-old-status")
async def clear_old_status():
    """Limpa status de processamento antigos (mais de 1 hora)"""
    try:
        current_time = datetime.utcnow()
        tasks_to_remove = []
        
        for task_id, status in processing_status.items():
            try:
                status_time = datetime.fromisoformat(status['timestamp'].replace('Z', '+00:00').replace('+00:00', ''))
                time_diff = (current_time - status_time).total_seconds()
                
                # Remove status com mais de 1 hora
                if time_diff > 3600:
                    tasks_to_remove.append(task_id)
            except:
                # Se houver erro ao parsear o timestamp, remove
                tasks_to_remove.append(task_id)
        
        for task_id in tasks_to_remove:
            del processing_status[task_id]
        
        return {
            "message": f"Limpeza concluída. {len(tasks_to_remove)} status antigos removidos.",
            "removed_tasks": len(tasks_to_remove),
            "active_tasks": len(processing_status),
            "timestamp": current_time.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro na limpeza de status: {e}")
        raise HTTPException(status_code=500, detail=f"Erro na limpeza: {str(e)}")

@app.get("/processing-status")
async def get_all_processing_status():
    """Retorna todos os status de processamento ativos"""
    try:
        return {
            "active_tasks": len(processing_status),
            "tasks": processing_status,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Erro ao obter status: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter status: {str(e)}")

@app.post("/force-complete-status/{task_id}")
async def force_complete_status(task_id: str):
    """Força a marcação de um status como concluído (para debug)"""
    try:
        if task_id not in processing_status:
            raise HTTPException(status_code=404, detail="Task ID não encontrado")
        
        processing_status[task_id]['status'] = ProcessingStatus.COMPLETED
        processing_status[task_id]['progress'] = 100
        processing_status[task_id]['message'] = "Processamento forçado como concluído"
        processing_status[task_id]['timestamp'] = datetime.utcnow().isoformat()
        
        return {
            "message": f"Status da task {task_id} forçado como concluído",
            "task_id": task_id,
            "status": processing_status[task_id],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao forçar conclusão: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao forçar conclusão: {str(e)}")

@app.get("/check-completion/{task_id}")
async def check_completion(task_id: str):
    """Verifica especificamente se uma tarefa foi concluída"""
    try:
        if task_id not in processing_status:
            raise HTTPException(status_code=404, detail="Task ID não encontrado")
        
        task_status = processing_status[task_id]
        is_completed = task_status.get('status') == ProcessingStatus.COMPLETED
        is_error = task_status.get('status') == ProcessingStatus.ERROR
        
        return {
            "task_id": task_id,
            "is_completed": is_completed,
            "is_error": is_error,
            "status": task_status.get('status'),
            "progress": task_status.get('progress', 0),
            "message": task_status.get('message', ''),
            "result": task_status.get('result'),
            "timestamp": task_status.get('timestamp'),
            "completion_check": {
                "completed": is_completed,
                "error": is_error,
                "can_stop_polling": is_completed or is_error
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao verificar conclusão: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao verificar conclusão: {str(e)}")

@app.post("/notify-completion")
async def notify_completion():
    """Endpoint para simular notificação de conclusão (para teste)"""
    try:
        completed_tasks = []
        for task_id, status in processing_status.items():
            if status.get('status') == ProcessingStatus.COMPLETED:
                completed_tasks.append({
                    "task_id": task_id,
                    "message": status.get('message', ''),
                    "timestamp": status.get('timestamp')
                })
        
        return {
            "completed_tasks": completed_tasks,
            "total_completed": len(completed_tasks),
            "message": f"Encontradas {len(completed_tasks)} tarefas concluídas",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro na notificação: {e}")
        raise HTTPException(status_code=500, detail=f"Erro na notificação: {str(e)}")

@app.post("/feedback")
async def submit_feedback(
    request: FeedbackRequest,
    user_id: str = Depends(get_user_id_dependency)
):
    """Endpoint para salvar feedback do usuário no DynamoDB"""
    try:
        # Usar user_id do request se fornecido, senão usar o padrão
        actual_user_id = request.user_id or user_id
        
        # Salvar no DynamoDB usando message_id como chat_id
        feedback_saved = dynamodb_service.save_feedback(
            chat_id=request.message_id,
            feedback_type=request.feedback_type,
            feedback_comment=request.comment or ""
        )
        
        if not feedback_saved:
            raise HTTPException(status_code=500, detail="Erro ao salvar feedback no DynamoDB")
        
        logger.info(f"Feedback salvo - Message ID: {request.message_id}, User: {actual_user_id}, Type: {'Positivo' if request.feedback_type == 0 else 'Negativo'}")
        
        return {
            "success": True,
            "message": "Feedback salvo com sucesso",
            "message_id": request.message_id,
            "feedback_type": request.feedback_type,
            "user_id": actual_user_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao salvar feedback: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao salvar feedback: {str(e)}")

@app.get("/feedback")
async def get_user_feedback(
    user_id: str = Depends(get_user_id_dependency),
    limit: int = 50
):
    """Obtém histórico de feedback do usuário"""
    try:
        feedbacks = dynamodb_service.get_user_feedback(user_id, limit)
        
        return {
            "user_id": user_id,
            "feedbacks": feedbacks,
            "total_feedbacks": len(feedbacks),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter feedback: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter feedback: {str(e)}")

@app.get("/debug/dynamodb")
async def debug_dynamodb():
    """Endpoint de debug para verificar status do DynamoDB"""
    try:
        # Verificar se DynamoDB está disponível
        is_available = dynamodb_service.is_available()
        
        # Tentar salvar um chat de teste
        test_chat_id = None
        test_save_error = None
        if is_available:
            try:
                test_chat_id = dynamodb_service.save_chat_interaction(
                    user_id="test_user",
                    pdf_name="test.pdf",
                    question="Test question",
                    answer="Test answer"
                )
            except Exception as e:
                test_save_error = str(e)
        
        # Tentar buscar chats de teste
        test_chats = []
        test_search_error = None
        if is_available:
            try:
                test_chats = dynamodb_service.get_chat_history("test_user", 5)
            except Exception as e:
                test_search_error = str(e)
        
        return {
            "dynamodb_available": is_available,
            "region": dynamodb_service.region,
            "tables": dynamodb_service.tables,
            "test_save": {
                "chat_id": test_chat_id,
                "error": test_save_error
            },
            "test_search": {
                "chats_found": len(test_chats),
                "chats": test_chats,
                "error": test_search_error
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro no debug DynamoDB: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
