import requests
import json
import logging
from typing import List, Dict, Any, Optional
import os
from urllib.parse import urljoin
from datetime import datetime

logger = logging.getLogger(__name__)

class ChromaDBClient:
    """Cliente para integração com o serviço ChromaDB via FastAPI"""
    
    def __init__(self, base_url: str = None):
        """
        Inicializa o cliente ChromaDB
        
        Args:
            base_url: URL base do serviço ChromaDB (ex: http://chromadb-service:8001)
        """
        self.base_url = base_url or os.getenv("CHROMADB_SERVICE_URL", "http://chromadb-service:8001")
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _make_request(self, method: str, endpoint: str, data: dict = None, params: dict = None) -> dict:
        """Faz uma requisição para o serviço ChromaDB"""
        try:
            url = urljoin(self.base_url, endpoint)
            print(f"DEBUG ChromaDB: Fazendo requisição {method} para {url}")
            
            # Timeout maior para operações de inserção que podem demorar
            timeout = 120 if method == "POST" and "add" in endpoint else 30
            print(f"DEBUG ChromaDB: Timeout definido: {timeout}s")
            
            if data and len(str(data)) > 1000:
                print(f"DEBUG ChromaDB: Payload grande: {len(str(data))} chars")
            
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=timeout
            )
            
            print(f"DEBUG ChromaDB: Resposta recebida: status {response.status_code}")
            response.raise_for_status()
            result = response.json()
            print(f"DEBUG ChromaDB: JSON parsado com sucesso")
            return result
            
        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout na requisição para ChromaDB: {e}"
            print(f"DEBUG ChromaDB: {error_msg}")
            logger.error(error_msg)
            raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            error_msg = f"Erro na requisição para ChromaDB: {e}"
            print(f"DEBUG ChromaDB: {error_msg}")
            logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Erro inesperado na comunicação com ChromaDB: {e}"
            print(f"DEBUG ChromaDB: {error_msg}")
            logger.error(error_msg)
            raise Exception(error_msg)
    
    def health_check(self) -> bool:
        """Verifica se o serviço ChromaDB está funcionando"""
        try:
            response = self._make_request("GET", "/health")
            return response.get("status") == "healthy"
        except Exception as e:
            logger.error(f"ChromaDB health check falhou: {e}")
            return False
    
    # Métodos para documentos e embeddings
    def add_documents(self, collection_name: str, documents: List[dict]) -> dict:
        """
        Adiciona documentos a uma coleção - DEPRECATED
        Use add_document_chunks em vez disso
        """
        print(f"DEBUG ChromaDB: add_documents DEPRECATED - use add_document_chunks")
        raise Exception("Método add_documents não suportado pela API atual. Use add_document_chunks.")
    
    def add_document_chunks(self, collection_name: str, pdf_name: str, chunks: List[str], 
                           metadata: dict = None) -> dict:
        """
        Adiciona chunks de texto de um PDF como documentos
        
        Args:
            collection_name: Nome da coleção
            pdf_name: Nome do PDF
            chunks: Lista de chunks de texto
            metadata: Metadados adicionais
        """
        print(f"DEBUG ChromaDB: add_document_chunks - {len(chunks)} chunks para {pdf_name}")
        
        documents = []
        for i, chunk in enumerate(chunks):
            doc_metadata = {
                "pdf_name": pdf_name,
                "chunk_index": i,
                "chunk_type": "text"
            }
            if metadata:
                doc_metadata.update(metadata)
            
            documents.append({
                "text": chunk,
                "metadata": doc_metadata,
                "chunk_id": f"{pdf_name}_chunk_{i}"
            })
        
        print(f"DEBUG ChromaDB: Documentos preparados, chamando endpoint /collections/{collection_name}/add")
        
        # Usar o endpoint correto da API ChromaDB
        return self._make_request("POST", f"/collections/{collection_name}/add", documents)
    
    def query_documents(self, collection_name: str, query_text: str, 
                       n_results: int = 5, filter_metadata: dict = None) -> dict:
        """
        Faz query semântica nos documentos
        
        Args:
            collection_name: Nome da coleção
            query_text: Texto da query
            n_results: Número máximo de resultados
            filter_metadata: Filtros de metadados
        """
        # Criar o payload exatamente como o endpoint espera
        data = {
            "query": query_text,
            "collection_name": collection_name,
            "n_results": n_results
        }
        
        if filter_metadata:
            data["where"] = filter_metadata
        
        print(f"DEBUG ChromaDB: Query payload: {data}")
        
        return self._make_request("POST", f"/collections/{collection_name}/query", data)
    
    def query_by_pdf(self, collection_name: str, query_text: str, pdf_name: str, 
                     n_results: int = 5) -> dict:
        """
        Faz query semântica filtrando por um PDF específico
        """
        filter_metadata = {"pdf_name": pdf_name}
        return self.query_documents(collection_name, query_text, n_results, filter_metadata)


class ChromaDBService:
    """Serviço para gerenciar embeddings RAG com ChromaDB"""
    
    def __init__(self, chromadb_url: str = None):
        """
        Inicializa o serviço ChromaDB
        
        Args:
            chromadb_url: URL do serviço ChromaDB
        """
        self.client = ChromaDBClient(chromadb_url)
        self.default_collection = "rag_documents"
        
        # Verificar conectividade
        if not self.client.health_check():
            logger.warning("ChromaDB não está acessível. Algumas funcionalidades podem não funcionar.")
    
    def initialize_default_collection(self) -> bool:
        """Inicializa a coleção padrão se não existir"""
        try:
            print(f"DEBUG ChromaDB: Assumindo que coleção '{self.default_collection}' será criada automaticamente")
            # O serviço ChromaDB cria a coleção automaticamente no primeiro add
            # Não precisamos verificar se existe
            logger.info(f"Coleção '{self.default_collection}' será gerenciada automaticamente pelo serviço")
            return True
                
        except Exception as e:
            print(f"DEBUG ChromaDB: Erro ao inicializar coleção: {e}")
            logger.error(f"Erro ao inicializar coleção padrão: {e}")
            return False
    
    def store_pdf_embeddings(self, pdf_name: str, text_chunks: List[str], 
                           user_id: str = None, pdf_metadata: dict = None) -> bool:
        """
        Armazena embeddings de um PDF
        
        Args:
            pdf_name: Nome do PDF
            text_chunks: Lista de chunks de texto
            user_id: ID do usuário (salvo nos metadados para controle de acesso)
            pdf_metadata: Metadados adicionais do PDF
        """
        try:
            if user_id:
                print(f"DEBUG ChromaDB: Iniciando store_pdf_embeddings para {pdf_name} com user_id: {user_id}")
            else:
                print(f"DEBUG ChromaDB: Iniciando store_pdf_embeddings para {pdf_name} SEM user_id")
            print(f"DEBUG ChromaDB: {len(text_chunks)} chunks a processar")
            
            # Garantir que a coleção existe
            print(f"DEBUG ChromaDB: Inicializando coleção padrão")
            if not self.initialize_default_collection():
                print(f"DEBUG ChromaDB: ERRO - Falha ao inicializar coleção")
                return False
            print(f"DEBUG ChromaDB: Coleção inicializada com sucesso")
            
            # Preparar metadados incluindo user_id
            print(f"DEBUG ChromaDB: Preparando metadados com user_id: {user_id}")
            base_metadata = {
                "pdf_name": pdf_name,
                "total_chunks": len(text_chunks),
                "indexed_at": datetime.utcnow().isoformat()
            }
            
            # Adicionar user_id aos metadados se fornecido
            if user_id:
                base_metadata["user_id"] = user_id
                print(f"DEBUG ChromaDB: user_id {user_id} adicionado aos metadados")
            #     base_metadata["user_id"] = user_id
            
            if pdf_metadata:
                base_metadata.update(pdf_metadata)
            
            print(f"DEBUG ChromaDB: Metadados preparados: {base_metadata}")
            
            # Adicionar chunks como documentos
            print(f"DEBUG ChromaDB: Chamando add_document_chunks")
            try:
                result = self.client.add_document_chunks(
                    collection_name=self.default_collection,
                    pdf_name=pdf_name,
                    chunks=text_chunks,
                    metadata=base_metadata
                )
                print(f"DEBUG ChromaDB: add_document_chunks concluído: {result}")
            except Exception as add_error:
                print(f"DEBUG ChromaDB: Erro em add_document_chunks: {add_error}")
                raise add_error
            
            print(f"DEBUG ChromaDB: Sucesso - {len(text_chunks)} chunks indexados")
            logger.info(f"Embeddings do PDF '{pdf_name}' armazenados com sucesso. {len(text_chunks)} chunks indexados.")
            return True
            
        except Exception as e:
            print(f"DEBUG ChromaDB: Erro geral em store_pdf_embeddings: {e}")
            logger.error(f"Erro ao armazenar embeddings do PDF '{pdf_name}': {e}")
            return False
    
    def search_similar_content(self, query: str, pdf_name: str = None, 
                              user_id: str = None, max_results: int = 5) -> List[dict]:
        """
        Busca conteúdo similar baseado em uma query
        
        Args:
            query: Texto da query
            pdf_name: Filtrar por PDF específico (opcional)
            user_id: Filtrar por usuário específico (opcional)
            max_results: Número máximo de resultados
        
        Returns:
            Lista de documentos similares com scores
        """
        try:
            if user_id:
                print(f"DEBUG ChromaDB: Buscando conteúdo similar para USER: '{user_id}' - query: '{query}', pdf_name: '{pdf_name}'")
            else:
                print(f"DEBUG ChromaDB: Buscando conteúdo similar GLOBALMENTE - query: '{query}', pdf_name: '{pdf_name}'")
            
            # Preparar filtros
            filter_metadata = {}
            if pdf_name:
                filter_metadata["pdf_name"] = pdf_name
            if user_id:
                filter_metadata["user_id"] = user_id
            
            print(f"DEBUG ChromaDB: Filtros aplicados: {filter_metadata}")
            
            # Fazer a query usando a API correta
            result = self.client.query_documents(
                collection_name=self.default_collection,
                query_text=query,
                n_results=max_results,
                filter_metadata=filter_metadata if filter_metadata else None
            )
            
            print(f"DEBUG ChromaDB: Resultado da query: {result}")
            
            # Processar resultados - ajustar para o formato da API ChromaDB
            documents = []
            if "documents" in result and result["documents"]:
                print(f"DEBUG ChromaDB: Processando {len(result['documents'])} documentos")
                for i, doc in enumerate(result["documents"]):
                    processed_doc = {
                        "text": doc,
                        "metadata": result.get("metadatas", [{}])[i] if "metadatas" in result else {},
                        "score": result.get("distances", [0])[i] if "distances" in result else 0,
                        "id": result.get("ids", [""])[i] if "ids" in result else ""
                    }
                    documents.append(processed_doc)
                    print(f"DEBUG ChromaDB: Documento {i}: {len(doc)} chars, score: {processed_doc['score']}")
            else:
                print(f"DEBUG ChromaDB: Nenhum documento retornado na resposta: {result}")
            
            logger.info(f"Busca realizada: encontrados {len(documents)} documentos similares")
            return documents
            
        except Exception as e:
            print(f"DEBUG ChromaDB: Erro na busca semântica: {e}")
            logger.error(f"Erro na busca semântica: {e}")
            return []
    
    def search_similar_content_global(self, query: str, pdf_name: str = None, max_results: int = 5) -> List[dict]:
        """
        Busca conteúdo similar baseado em uma query - ACESSO GLOBAL (todos os usuários)
        
        Args:
            query: Texto da query
            pdf_name: Filtrar por PDF específico (opcional)
            max_results: Número máximo de resultados
        
        Returns:
            Lista de documentos similares com scores
        """
        try:
            print(f"DEBUG ChromaDB: Buscando conteúdo similar GLOBALMENTE - query: '{query}', pdf_name: '{pdf_name}'")
            
            # Preparar filtros - SEM user_id para busca global
            filter_metadata = {}
            if pdf_name:
                filter_metadata["pdf_name"] = pdf_name
            
            print(f"DEBUG ChromaDB: Filtros aplicados (SEM user_id): {filter_metadata}")
            
            # Fazer a query usando a API correta
            result = self.client.query_documents(
                collection_name=self.default_collection,
                query_text=query,
                n_results=max_results,
                filter_metadata=filter_metadata if filter_metadata else None
            )
            
            print(f"DEBUG ChromaDB: Resultado da query: {result}")
            
            # Processar resultados
            documents = []
            if "documents" in result and result["documents"]:
                print(f"DEBUG ChromaDB: Processando {len(result['documents'])} documentos")
                for i, doc in enumerate(result["documents"]):
                    processed_doc = {
                        "text": doc,
                        "metadata": result.get("metadatas", [{}])[i] if "metadatas" in result else {},
                        "score": result.get("distances", [0])[i] if "distances" in result else 0,
                        "id": result.get("ids", [""])[i] if "ids" in result else ""
                    }
                    documents.append(processed_doc)
                    print(f"DEBUG ChromaDB: Documento {i}: {len(doc)} chars, score: {processed_doc['score']}")
            else:
                print(f"DEBUG ChromaDB: Nenhum documento retornado na resposta: {result}")
            
            logger.info(f"Busca global realizada: encontrados {len(documents)} documentos similares")
            return documents
            
        except Exception as e:
            print(f"DEBUG ChromaDB: Erro na busca semântica global: {e}")
            logger.error(f"Erro na busca semântica global: {e}")
            return []
    
    def delete_pdf_embeddings(self, pdf_name: str, user_id: str = None) -> bool:
        """
        Remove todos os embeddings de um PDF
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário (para validação)
        """
        try:
            print(f"DEBUG ChromaDB: Tentando deletar embeddings do PDF '{pdf_name}'")
            # Por enquanto, vamos apenas log que a função foi chamada
            # Implementação completa dependeria de endpoint de delete específico
            logger.info(f"Solicitação de remoção de embeddings do PDF '{pdf_name}' (funcionalidade limitada)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao remover embeddings do PDF '{pdf_name}': {e}")
            return False
    
    def get_pdf_chunks(self, pdf_name: str, user_id: str = None) -> List[dict]:
        """
        Obtém todos os chunks de um PDF específico - ACESSO GLOBAL
        
        Args:
            pdf_name: Nome do PDF
            user_id: IGNORADO - busca global
        """
        try:
            # Filtrar apenas por PDF, sem user_id para acesso global
            filter_metadata = {"pdf_name": pdf_name}
            
            print(f"DEBUG ChromaDB: Buscando chunks do PDF '{pdf_name}' GLOBALMENTE (user_id ignorado)")
            
            # Usar uma query genérica para obter todos os chunks do PDF
            result = self.client.query_documents(
                collection_name=self.default_collection,
                query_text="documento",  # Query genérica para buscar documentos
                n_results=1000,  # Limite alto para pegar todos os chunks
                filter_metadata=filter_metadata
            )
            
            # Reformatar os resultados
            documents = []
            if "documents" in result and result["documents"]:
                for i, doc in enumerate(result["documents"]):
                    documents.append({
                        "text": doc,
                        "metadata": result.get("metadatas", [{}])[i] if "metadatas" in result else {},
                        "id": result.get("ids", [""])[i] if "ids" in result else ""
                    })
            
            return documents
            
        except Exception as e:
            logger.error(f"Erro ao obter chunks do PDF '{pdf_name}': {e}")
            return []
    
    def get_collection_info(self) -> dict:
        """Obtém informações sobre a coleção padrão"""
        try:
            # Fazer uma query simples para verificar se a coleção tem dados
            result = self.client.query_documents(
                collection_name=self.default_collection,
                query_text="teste",
                n_results=1
            )
            return {"status": "active", "has_data": bool(result.get("documents"))}
        except Exception as e:
            logger.error(f"Erro ao obter informações da coleção: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_indexed_pdfs(self, user_id: str = None) -> List[str]:
        """
        Lista todos os PDFs indexados - ACESSO GLOBAL
        
        Args:
            user_id: IGNORADO - lista todos os PDFs globalmente
        """
        try:
            print(f"DEBUG ChromaDB: Listando PDFs indexados GLOBALMENTE (user_id ignorado)")
            
            # Fazer uma query genérica para obter documentos de TODOS os usuários
            result = self.client.query_documents(
                collection_name=self.default_collection,
                query_text="documento",  # Query genérica
                n_results=1000,
                filter_metadata=None  # SEM filtros para busca global
            )
            
            # Extrair nomes únicos de PDFs
            pdf_names = set()
            if "metadatas" in result and result["metadatas"]:
                for metadata in result["metadatas"]:
                    if metadata and "pdf_name" in metadata:
                        pdf_names.add(metadata["pdf_name"])
            
            return list(pdf_names)
            
        except Exception as e:
            logger.error(f"Erro ao listar PDFs indexados: {e}")
            return []
