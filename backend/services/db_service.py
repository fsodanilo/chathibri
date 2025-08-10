from services.dynamodb_service import DynamoDBService
from services.chromadb_client import ChromaDBService
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class DBService:
    def __init__(self):
        """Inicializa o serviço de banco de dados com DynamoDB e ChromaDB"""
        self.dynamodb = DynamoDBService()
        self.chromadb = ChromaDBService()

    def recent_chats(self, user_id: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Obtém chats recentes
        
        Args:
            user_id: ID do usuário (obrigatório para DynamoDB)
            limit: Número máximo de chats
        
        Returns:
            Lista de chats recentes
        """
        try:
            if not user_id:
                logger.warning("user_id não fornecido para recent_chats. Retornando lista vazia.")
                return []
            
            chats = self.dynamodb.get_recent_chats(user_id, limit)
            
            # Formatar dados para compatibilidade
            formatted_chats = []
            for chat in chats:
                formatted_chat = {
                    "pdf_name": chat.get("pdf_name", ""),
                    "question": chat.get("question", ""),
                    "answer": chat.get("answer", ""),
                    "timestamp": chat.get("timestamp", ""),
                    "chat_id": chat.get("chat_id", "")
                }
                formatted_chats.append(formatted_chat)
            
            return formatted_chats
            
        except Exception as e:
            logger.error(f"Erro ao obter chats recentes: {e}")
            return []

    def create_table_from_pdf(self, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Cria "tabela" de chunks de PDF (armazenados no ChromaDB)
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            Dicionário com resultado da operação
        """
        try:
            # Obter chunks do PDF do ChromaDB
            chunks = self.chromadb.get_pdf_chunks(pdf_name, user_id)
            
            if not chunks:
                return {
                    "error": f"PDF '{pdf_name}' não encontrado ou sem conteúdo indexado",
                    "chunks_found": 0
                }
            
            # Os chunks já estão "tabelados" no ChromaDB, então apenas retornamos informações
            chunk_data = []
            for i, chunk in enumerate(chunks):
                chunk_info = {
                    "chunk_index": i,
                    "text": chunk.get("text", "")[:200] + "..." if len(chunk.get("text", "")) > 200 else chunk.get("text", ""),
                    "full_text_length": len(chunk.get("text", "")),
                    "metadata": chunk.get("metadata", {}),
                    "chunk_id": chunk.get("id", "")
                }
                chunk_data.append(chunk_info)
            
            # Salvar metadados no DynamoDB se necessário
            if user_id:
                table_metadata = {
                    "total_chunks": len(chunks),
                    "created_from_pdf": pdf_name,
                    "operation": "create_table_from_pdf",
                    "chunks_indexed": len(chunks)
                }
                
                try:
                    self.dynamodb.save_pdf_metadata(user_id, pdf_name, table_metadata)
                except Exception as e:
                    logger.warning(f"Erro ao salvar metadados: {e}")
            
            return {
                "message": f"Tabela criada a partir do PDF {pdf_name}",
                "chunks_found": len(chunks),
                "chunks": chunk_data[:10],  # Retorna apenas os primeiros 10 para evitar resposta muito grande
                "total_chunks": len(chunks),
                "pdf_name": pdf_name
            }
            
        except Exception as e:
            logger.error(f"Erro ao criar tabela do PDF '{pdf_name}': {e}")
            return {
                "error": f"Erro ao processar PDF '{pdf_name}': {e}",
                "chunks_found": 0
            }

    def get_pdf_content(self, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Obtém conteúdo de um PDF do ChromaDB
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            Dicionário com conteúdo do PDF
        """
        try:
            chunks = self.chromadb.get_pdf_chunks(pdf_name, user_id)
            
            if not chunks:
                return {
                    "error": f"PDF '{pdf_name}' não encontrado",
                    "content": ""
                }
            
            # Combinar todos os chunks em um texto único
            full_content = "\n\n".join([chunk.get("text", "") for chunk in chunks])
            
            return {
                "name": pdf_name,
                "content": full_content,
                "chunks_count": len(chunks),
                "total_length": len(full_content)
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter conteúdo do PDF '{pdf_name}': {e}")
            return {
                "error": f"Erro ao obter PDF: {e}",
                "content": ""
            }

    def store_pdf_content(self, pdf_name: str, content: str, user_id: str = None, 
                         chunks: List[str] = None) -> bool:
        """
        Armazena conteúdo de PDF no ChromaDB
        
        Args:
            pdf_name: Nome do PDF
            content: Conteúdo completo do PDF
            user_id: ID do usuário
            chunks: Lista de chunks (se não fornecida, será criada automaticamente)
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            # Se chunks não foi fornecido, criar chunks básicos
            if not chunks:
                # Dividir conteúdo em chunks de aproximadamente 1000 caracteres
                chunk_size = 1000
                chunks = []
                for i in range(0, len(content), chunk_size):
                    chunk = content[i:i + chunk_size]
                    if chunk.strip():  # Apenas chunks não vazios
                        chunks.append(chunk)
            
            # Armazenar no ChromaDB
            success = self.chromadb.store_pdf_embeddings(
                pdf_name=pdf_name,
                text_chunks=chunks,
                user_id=user_id,
                pdf_metadata={
                    "total_content_length": len(content),
                    "original_chunks": len(chunks)
                }
            )
            
            # Salvar metadados no DynamoDB
            if success and user_id:
                metadata = {
                    "content_length": len(content),
                    "chunks_count": len(chunks),
                    "indexed_in_chromadb": True
                }
                try:
                    self.dynamodb.save_pdf_metadata(user_id, pdf_name, metadata)
                except Exception as e:
                    logger.warning(f"Erro ao salvar metadados no DynamoDB: {e}")
            
            return success
            
        except Exception as e:
            logger.error(f"Erro ao armazenar PDF '{pdf_name}': {e}")
            return False

    def delete_pdf_content(self, pdf_name: str, user_id: str = None) -> bool:
        """
        Remove conteúdo de PDF do ChromaDB
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            # Remover do ChromaDB
            success = self.chromadb.delete_pdf_embeddings(pdf_name, user_id)
            
            if success:
                logger.info(f"PDF '{pdf_name}' removido do ChromaDB")
            
            return success
            
        except Exception as e:
            logger.error(f"Erro ao remover PDF '{pdf_name}': {e}")
            return False

    def list_user_pdfs(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Lista PDFs de um usuário
        
        Args:
            user_id: ID do usuário
        
        Returns:
            Lista de PDFs do usuário
        """
        try:
            # Obter PDFs do DynamoDB
            pdfs_dynamodb = self.dynamodb.get_user_pdfs(user_id)
            
            # Obter PDFs indexados no ChromaDB
            pdfs_chromadb = self.chromadb.list_indexed_pdfs(user_id)
            
            # Combinar informações
            pdfs_info = []
            
            # Criar dict para acesso rápido aos metadados do DynamoDB
            dynamodb_pdfs = {pdf.get("pdf_name", ""): pdf for pdf in pdfs_dynamodb}
            
            # Processar PDFs indexados
            for pdf_name in pdfs_chromadb:
                pdf_info = {
                    "pdf_name": pdf_name,
                    "indexed_in_chromadb": True,
                    "metadata": dynamodb_pdfs.get(pdf_name, {}).get("metadata", {}),
                    "created_at": dynamodb_pdfs.get(pdf_name, {}).get("created_at", ""),
                }
                pdfs_info.append(pdf_info)
            
            # Adicionar PDFs que estão apenas no DynamoDB
            for pdf_name, pdf_data in dynamodb_pdfs.items():
                if pdf_name not in pdfs_chromadb:
                    pdf_info = {
                        "pdf_name": pdf_name,
                        "indexed_in_chromadb": False,
                        "metadata": pdf_data.get("metadata", {}),
                        "created_at": pdf_data.get("created_at", ""),
                    }
                    pdfs_info.append(pdf_info)
            
            return pdfs_info
            
        except Exception as e:
            logger.error(f"Erro ao listar PDFs do usuário {user_id}: {e}")
            return []

    def list_pdfs(self) -> List[Dict[str, Any]]:
        """
        Lista PDFs usando DynamoDB
        
        Returns:
            Lista de dicionários com pdf_name e total_words
        """
        try:
            # Obter PDFs completos do DynaomoDB
            full_pdfs = self.dynamodb.get_full_pdfs()
            
            # Extrair apenas pdf_name e total_words
            pdfs_list = []
            for pdf in full_pdfs:
                pdf_info = {
                    "pdf_name": pdf.get("pdf_name", ""),
                    "total_words": pdf.get("metadata", {}).get("total_words", 0)
                }
                pdfs_list.append(pdf_info)
            
            return pdfs_list
            
        except Exception as e:
            logger.error(f"Erro ao listar PDFs: {e}")
            return []

    def get_database_stats(self, user_id: str = None) -> Dict[str, Any]:
        """
        Obtém estatísticas dos bancos de dados
        
        Args:
            user_id: ID do usuário (opcional)
        
        Returns:
            Dicionário com estatísticas
        """
        try:
            stats = {
                "chromadb": {},
                "dynamodb": {},
                "user_specific": {}
            }
            
            # Estatísticas do ChromaDB
            try:
                chromadb_info = self.chromadb.get_collection_info()
                stats["chromadb"] = chromadb_info
            except Exception as e:
                stats["chromadb"] = {"error": str(e)}
            
            # Estatísticas específicas do usuário
            if user_id:
                try:
                    user_pdfs = self.list_user_pdfs(user_id)
                    user_chats = self.recent_chats(user_id, limit=1000)  # Obter todos
                    
                    stats["user_specific"] = {
                        "user_id": user_id,
                        "total_pdfs": len(user_pdfs),
                        "total_chats": len(user_chats),
                        "pdfs_indexed": len([p for p in user_pdfs if p.get("indexed_in_chromadb", False)])
                    }
                except Exception as e:
                    stats["user_specific"] = {"error": str(e)}
            
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {"error": str(e)}