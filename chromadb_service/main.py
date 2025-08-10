from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import chromadb
from chromadb.config import Settings
import uuid
import logging
from datetime import datetime
import os
from sentence_transformers import SentenceTransformer

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ChromaDB RAG Service",
    description="Serviço para gerenciar embeddings RAG com ChromaDB",
    version="0.0.7"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurar ChromaDB com fallback para diretório alternativo
CHROMA_DATA_PATH = "/app/chroma_data"
CHROMA_FALLBACK_PATH = "/tmp/chroma_data"

# Inicializar cliente ChromaDB de forma lazy
client = None

def get_writable_chroma_path():
    """Encontra um diretório gravável para o ChromaDB"""
    paths_to_try = [
        CHROMA_DATA_PATH,
        CHROMA_FALLBACK_PATH,
        "/tmp/chromadb_fallback",
        os.path.expanduser("~/chroma_data")
    ]
    
    for path in paths_to_try:
        try:
            # Tentar criar o diretório se não existir
            os.makedirs(path, mode=0o755, exist_ok=True)
            
            # Testar escrita
            test_file = os.path.join(path, '.test_write')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            
            logger.info(f"Diretório ChromaDB disponível: {path}")
            return path
            
        except Exception as e:
            logger.warning(f"Não foi possível usar {path}: {e}")
            continue
    
    raise RuntimeError("Nenhum diretório gravável encontrado para ChromaDB")

def get_chromadb_client():
    """Inicializa o cliente ChromaDB com lazy loading e fallback de diretório"""
    global client
    if client is None:
        try:
            # Encontrar diretório gravável
            data_path = get_writable_chroma_path()
            
            logger.info(f"Inicializando cliente ChromaDB em: {data_path}")
            client = chromadb.PersistentClient(
                path=data_path,
                settings=Settings(
                    anonymized_telemetry=False,
                    allow_reset=True
                )
            )
            logger.info("Cliente ChromaDB inicializado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inicializar cliente ChromaDB: {str(e)}")
            # Fallback para cliente em memória se tudo falhar
            logger.warning("Usando cliente ChromaDB em memória como fallback")
            try:
                client = chromadb.EphemeralClient(
                    settings=Settings(
                        anonymized_telemetry=False,
                        allow_reset=True
                    )
                )
                logger.info("Cliente ChromaDB em memória inicializado")
            except Exception as mem_error:
                logger.error(f"Falha também no cliente em memória: {str(mem_error)}")
                raise e
    return client

# Inicializar modelo de embeddings de forma robusta
embedding_model = None

def get_embedding_model():
    """Inicializa o modelo de embeddings com lazy loading"""
    global embedding_model
    if embedding_model is None:
        try:
            logger.info("Inicializando modelo de embeddings...")
            embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            logger.info("Modelo de embeddings inicializado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inicializar modelo de embeddings: {str(e)}")
            raise e
    return embedding_model

# Modelos Pydantic
class DocumentChunk(BaseModel):
    text: str
    metadata: Dict[str, Any] = {}
    chunk_id: Optional[str] = None

class QueryRequest(BaseModel):
    query: str
    collection_name: str = "rag_documents"
    n_results: int = 5
    where: Optional[Dict[str, Any]] = None

class QueryResponse(BaseModel):
    documents: List[str]
    metadatas: List[Dict[str, Any]]
    distances: List[float]
    ids: List[str]

class CollectionInfo(BaseModel):
    name: str
    count: int
    metadata: Optional[Dict[str, Any]] = None

# Funções auxiliares
def get_or_create_collection(name: str):
    """Obtém ou cria uma coleção no ChromaDB"""
    client = get_chromadb_client()
    try:
        return client.get_collection(name=name)
    except Exception:
        return client.create_collection(
            name=name,
            metadata={"created_at": datetime.now().isoformat()}
        )

def generate_embedding(text: str) -> List[float]:
    """Gera embedding para um texto"""
    model = get_embedding_model()
    return model.encode(text).tolist()

# Endpoints
@app.get("/")
async def root():
    client = get_chromadb_client()
    return {
        "message": "ChromaDB RAG Service is running",
        "timestamp": datetime.now().isoformat(),
        "collections": len(client.list_collections())
    }

@app.get("/health")
async def health_check():
    """Endpoint de verificação de saúde"""
    try:
        collections = get_chromadb_client().list_collections()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "collections_count": len(collections),
            "embedding_model": "all-MiniLM-L6-v2"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ChromaDB error: {str(e)}")

@app.post("/collections/{collection_name}/add")
async def add_documents(collection_name: str, chunks: List[DocumentChunk]):
    """Adiciona documentos à coleção"""
    try:
        collection = get_or_create_collection(collection_name)
        
        # Preparar dados para inserção
        documents = []
        metadatas = []
        ids = []
        embeddings = []
        
        for chunk in chunks:
            # Gerar ID único se não fornecido
            chunk_id = chunk.chunk_id or str(uuid.uuid4())
            
            # Gerar embedding
            embedding = generate_embedding(chunk.text)
            
            # Adicionar timestamp aos metadados
            metadata = chunk.metadata.copy()
            metadata.update({
                "added_at": datetime.now().isoformat(),
                "text_length": len(chunk.text)
            })
            
            documents.append(chunk.text)
            metadatas.append(metadata)
            ids.append(chunk_id)
            embeddings.append(embedding)
        
        # Inserir no ChromaDB
        collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids,
            embeddings=embeddings
        )
        
        logger.info(f"Adicionados {len(chunks)} documentos à coleção {collection_name}")
        
        return {
            "message": f"Adicionados {len(chunks)} documentos",
            "collection": collection_name,
            "document_ids": ids
        }
        
    except Exception as e:
        logger.error(f"Erro ao adicionar documentos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/collections/{collection_name}/query", response_model=QueryResponse)
async def query_documents(collection_name: str, request: QueryRequest):
    """Busca documentos similares na coleção"""
    try:
        collection = get_or_create_collection(collection_name)
        
        # Gerar embedding da query
        query_embedding = generate_embedding(request.query)
        
        # Realizar busca
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=request.n_results,
            where=request.where,
            include=["documents", "metadatas", "distances"]
        )
        
        logger.info(f"Query realizada na coleção {collection_name}: {len(results['documents'][0])} resultados")
        
        return QueryResponse(
            documents=results['documents'][0],
            metadatas=results['metadatas'][0],
            distances=results['distances'][0],
            ids=results['ids'][0]
        )
        
    except Exception as e:
        logger.error(f"Erro na query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collections")
async def list_collections():
    """Lista todas as coleções"""
    try:
        collections = get_chromadb_client().list_collections()
        
        collection_info = []
        for col in collections:
            info = CollectionInfo(
                name=col.name,
                count=col.count(),
                metadata=col.metadata
            )
            collection_info.append(info)
        
        return {
            "collections": collection_info,
            "total": len(collection_info)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collections/{collection_name}")
async def get_collection_info(collection_name: str):
    """Obtém informações de uma coleção específica"""
    try:
        collection = get_chromadb_client().get_collection(collection_name)
        
        return {
            "name": collection.name,
            "count": collection.count(),
            "metadata": collection.metadata
        }
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Coleção não encontrada: {str(e)}")

@app.delete("/collections/{collection_name}")
async def delete_collection(collection_name: str):
    """Deleta uma coleção"""
    try:
        get_chromadb_client().delete_collection(collection_name)
        
        return {
            "message": f"Coleção {collection_name} deletada com sucesso"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/collections/{collection_name}/documents")
async def delete_documents(collection_name: str, document_ids: List[str]):
    """Remove documentos específicos de uma coleção"""
    try:
        collection = get_chromadb_client().get_collection(collection_name)
        collection.delete(ids=document_ids)
        
        return {
            "message": f"Removidos {len(document_ids)} documentos da coleção {collection_name}",
            "deleted_ids": document_ids
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/collections/{collection_name}/reset")
async def reset_collection(collection_name: str):
    """Reseta uma coleção (remove todos os documentos)"""
    try:
        # Deletar e recriar a coleção
        try:
            get_chromadb_client().delete_collection(collection_name)
        except:
            pass  # Coleção pode não existir
        
        collection = get_chromadb_client().create_collection(
            name=collection_name,
            metadata={"created_at": datetime.now().isoformat()}
        )
        
        return {
            "message": f"Coleção {collection_name} resetada com sucesso",
            "count": collection.count()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Obtém estatísticas gerais do ChromaDB"""
    try:
        collections = get_chromadb_client().list_collections()
        total_documents = sum(col.count() for col in collections)
        
        stats = {
            "total_collections": len(collections),
            "total_documents": total_documents,
            "collections_info": [
                {
                    "name": col.name,
                    "count": col.count(),
                    "metadata": col.metadata
                }
                for col in collections
            ],
            "embedding_model": "all-MiniLM-L6-v2",
            "timestamp": datetime.now().isoformat()
        }
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
