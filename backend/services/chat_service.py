from langchain_google_genai import GoogleGenerativeAI
from langchain_aws import ChatBedrock
from langchain.chains.question_answering import load_qa_chain
from services.dynamodb_service import DynamoDBService
from services.chromadb_client import ChromaDBService
import os
import google.generativeai as genai
import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


class ChatService:
    def __init__(self, use_bedrock: bool = True):
        """Inicializa o serviço de chat com DynamoDB e ChromaDB"""
        self.use_bedrock = use_bedrock
        
        # Inicializar serviços
        self.dynamodb = DynamoDBService()
        self.chromadb = ChromaDBService()
        
        # Configurar modelos LLM
        self.setup_llm_models()
        
        # Verificar se os serviços estão funcionando
        if not self.chromadb.client.health_check():
            logger.warning("ChromaDB não está acessível. Funcionalidade RAG limitada.")
    
    def setup_llm_models(self):
        """Configura modelos LLM (Bedrock e Google AI)"""
        self.bedrock_llm = None
        self.google_llm = None
        
        # Configura AWS Bedrock primeiro
        if self.use_bedrock:
            try:
                self.bedrock_llm = ChatBedrock(
                    model_id="anthropic.claude-3-5-sonnet-20240620-v1:0",
                    model_kwargs={
                        "max_tokens": 4000,
                        "temperature": 0.3,
                        "top_p": 1,
                        "stop_sequences": [],
                    },
                    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
                )
                print("AWS Bedrock (Claude 3.5 Sonnet) configurado para chat")
            except Exception as e:
                print(f"Erro ao configurar Bedrock para chat: {e}")
                self.bedrock_llm = None
        
        # Configura Google AI como fallback
        try:
            self.google_llm = GoogleGenerativeAI(model="gemini-2.5-flash-lite-preview-06-17", temperature=0.3)
            print("Google AI configurada como fallback para chat")
        except Exception as e:
            print(f"Erro ao configurar Google AI para chat: {e}")
            self.google_llm = None
        
        # Determina qual modelo usar
        if self.bedrock_llm:
            self.llm = self.bedrock_llm
            self.model_type = "bedrock"
        elif self.google_llm:
            self.llm = self.google_llm
            self.model_type = "google"
        else:
            logger.error("Nenhum modelo LLM configurado. Chat não funcionará.")
            self.llm = None
            self.model_type = None
        
        # Carrega chain QA se modelo disponível
        if self.llm:
            self.qa_chain = load_qa_chain(self.llm, chain_type="stuff")

    def ask_question(self, question: str, pdf_name: str, user_id: str = None, context_override: str = None) -> Dict[str, Any]:
        """
        Processa uma pergunta usando RAG com ChromaDB
        
        Args:
            question: Pergunta do usuário
            pdf_name: Nome do PDF para consultar
            user_id: ID do usuário (opcional)
            context_override: Contexto pré-processado para usar (opcional)
        
        Returns:
            Dicionário com pergunta, resposta e metadados
        """
        import time
        start_time = time.time()
        
        try:
            # Se contexto foi fornecido, usar ele diretamente
            if context_override:
                combined_context = context_override
                sources = []
                # Criar sources formatados para o frontend
                for i, chunk in enumerate(context_override.split('\n\n')[:5]):  # Máximo 5 chunks
                    sources.append({
                        "text": chunk[:200] + "..." if len(chunk) > 200 else chunk,
                        "pdf_name": pdf_name,
                        "chunk_index": i,
                        "similarity_score": 1.0
                    })
            else:
                # Buscar conteúdo similar no ChromaDB
                similar_docs = self.chromadb.search_similar_content(
                    query=question,
                    pdf_name=pdf_name,
                    user_id=user_id,
                    max_results=5
                )
                
                if not similar_docs:
                    error_msg = f"Nenhum conteúdo encontrado para o PDF '{pdf_name}'"
                    logger.warning(error_msg)
                    return {
                        "question": question,
                        "answer": error_msg,
                        "error": "PDF não encontrado ou sem conteúdo indexado",
                        "sources": []
                    }
                
                # Preparar contexto para o LLM
                context_texts = []
                sources = []
                
                for doc in similar_docs:
                    context_texts.append(doc["text"])
                    metadata = doc.get("metadata", {})
                    sources.append({
                        "text": doc["text"][:200] + "..." if len(doc["text"]) > 200 else doc["text"],
                        "pdf_name": metadata.get("pdf_name", pdf_name),
                        "chunk_index": metadata.get("chunk_index", 0),
                        "similarity_score": doc.get("score", 0)
                    })
                
                # Combinar contextos
                combined_context = "\n\n".join(context_texts)
            
            # Criar prompt melhorado
            enhanced_prompt = f"""
            Com base no seguinte contexto extraído do documento '{pdf_name}', responda à pergunta de forma clara e detalhada.
            
            CONTEXTO:
            {combined_context}
            
            PERGUNTA: {question}
            
            RESPOSTA:
            """
            
            # Gerar resposta usando o modelo LLM
            try:
                response = self.llm.invoke(enhanced_prompt)
                # Extrair conteúdo baseado no tipo de modelo
                if self.use_bedrock and hasattr(response, 'content'):
                    answer = response.content
                elif isinstance(response, str):
                    answer = response
                else:
                    answer = str(response)
            except Exception as e:
                logger.error(f"Erro ao gerar resposta com LLM: {e}")
                answer = f"Erro ao processar a pergunta. Contexto encontrado mas falha na geração da resposta: {e}"
            
            # Salvar interação no DynamoDB
            chat_id = None
            processing_time = time.time() - start_time
            
            # Determinar modelo usado
            model_used = "bedrock-claude-3.5-sonnet" if self.use_bedrock and self.bedrock_llm else "google-gemini-2.5-flash"
            
            if user_id:
                try:
                    chat_id = self.dynamodb.save_chat_interaction(
                        user_id=user_id,
                        pdf_name=pdf_name,
                        question=question,
                        answer=answer,
                        metadata={
                            "num_sources": len(sources),
                            "chromadb_collection": self.chromadb.default_collection,
                            "model_used": model_used,
                            "processing_time_seconds": Decimal(str(round(processing_time, 3))),
                            "processing_time_ms": Decimal(str(round(processing_time * 1000, 1)))
                        }
                    )
                except Exception as e:
                    logger.error(f"Erro ao salvar interação no DynamoDB: {e}")
            
            result = {
                "question": question,
                "answer": answer,
                "sources": sources,
                "metadata": {
                    "pdf_name": pdf_name,
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "num_sources_found": len(sources),
                    "total_context_length": len(combined_context),
                    "model_used": model_used,
                    "processing_time_seconds": round(processing_time, 3),
                    "processing_time_ms": round(processing_time * 1000, 1)
                }
            }
            
            logger.info(f"Pergunta processada com sucesso. PDF: {pdf_name}, Fontes: {len(sources)}, Tempo: {processing_time:.3f}s")
            return result
            
        except Exception as e:
            error_msg = f"Erro ao processar pergunta: {e}"
            logger.error(error_msg)
            return {
                "question": question,
                "answer": error_msg,
                "error": str(e),
                "sources": []
            }

    def get_chat_history(self, user_id: str, pdf_name: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Obtém histórico de chat do usuário
        
        Args:
            user_id: ID do usuário
            pdf_name: Filtrar por PDF específico (opcional, mas não usado nesta versão)
            limit: Número máximo de chats
        
        Returns:
            Lista de interações de chat
        """
        try:
            # Usar o novo método que busca apenas por user_id e timestamp
            chats = self.dynamodb.get_chat_history(user_id, limit)
            
            # Processar e formatar dados
            formatted_chats = []
            for chat in chats:
                formatted_chat = {
                    "chat_id": chat.get("chat_id"),
                    "pdf_name": chat.get("pdf_name"),
                    "question": chat.get("question"),
                    "answer": chat.get("answer"),
                    "timestamp": chat.get("timestamp"),
                    "metadata": chat.get("metadata", {})
                }
                formatted_chats.append(formatted_chat)
            
            return formatted_chats
            
        except Exception as e:
            logger.error(f"Erro ao obter histórico de chat: {e}")
            return []

    def get_recent_chats(self, user_id: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Obtém chats recentes (compatibilidade com interface existente)
        
        Args:
            user_id: ID do usuário (se None, tenta obter de todos os usuários)
            limit: Número máximo de chats
        """
        if user_id:
            return self.get_chat_history(user_id, limit=limit)
        else:
            # Para compatibilidade, retorna uma lista vazia se não houver user_id
            logger.warning("get_recent_chats chamado sem user_id. Retornando lista vazia.")
            return []

    def create_conversation(self, user_id: str, pdf_name: str, title: str = None) -> str:
        """
        Cria uma nova conversa
        
        Args:
            user_id: ID do usuário
            pdf_name: Nome do PDF
            title: Título da conversa (opcional)
        
        Returns:
            ID da conversa criada
        """
        try:
            conversation_data = {
                "title": title or f"Chat sobre {pdf_name}",
                "pdf_name": pdf_name,
                "status": "active"
            }
            
            conversation_id = self.dynamodb.create_conversation(user_id, conversation_data)
            logger.info(f"Conversa {conversation_id} criada para usuário {user_id}")
            return conversation_id
            
        except Exception as e:
            logger.error(f"Erro ao criar conversa: {e}")
            raise

    def delete_chat_history(self, user_id: str, pdf_name: str = None) -> bool:
        """
        Deleta histórico de chat
        
        Args:
            user_id: ID do usuário
            pdf_name: PDF específico (opcional)
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            # Implementar lógica de deleção no DynamoDB
            # Por enquanto, apenas log a ação
            logger.info(f"Solicitação de deleção de histórico: user={user_id}, pdf={pdf_name}")
            
            # TODO: Implementar deleção real no DynamoDB
            # Seria necessário buscar e deletar itens específicos
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao deletar histórico: {e}")
            return False

    def get_pdf_stats(self, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Obtém estatísticas de um PDF
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário (opcional)
        
        Returns:
            Dicionário com estatísticas
        """
        try:
            # Obter chunks do ChromaDB
            chunks = self.chromadb.get_pdf_chunks(pdf_name, user_id)
            
            # Obter histórico de chat do DynamoDB
            chat_history = []
            if user_id:
                chat_history = self.get_chat_history(user_id, pdf_name)
            
            stats = {
                "pdf_name": pdf_name,
                "total_chunks": len(chunks),
                "total_chats": len(chat_history),
                "indexed_in_chromadb": len(chunks) > 0,
                "has_chat_history": len(chat_history) > 0
            }
            
            if chunks:
                # Calcular estatísticas dos chunks
                total_chars = sum(len(chunk.get("text", "")) for chunk in chunks)
                stats.update({
                    "total_characters": total_chars,
                    "avg_chunk_size": total_chars / len(chunks) if chunks else 0
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas do PDF: {e}")
            return {"error": str(e)}

    def ask_question_general(self, question: str, user_id: str = None, context_override: str = None) -> Dict[str, Any]:
        """
        Processa uma pergunta geral (sem PDF específico) usando RAG com ChromaDB
        
        Args:
            question: Pergunta do usuário
            user_id: ID do usuário (opcional)
            context_override: Contexto pré-processado para usar (opcional)
        
        Returns:
            Dicionário com pergunta, resposta e metadados
        """
        import time
        start_time = time.time()
        
        try:
            # Se contexto foi fornecido, usar ele diretamente
            if context_override:
                combined_context = context_override
                sources = []
                # Criar sources formatados para o frontend
                for i, chunk in enumerate(context_override.split('\n\n')[:5]):  # Máximo 5 chunks
                    sources.append({
                        "text": chunk[:200] + "..." if len(chunk) > 200 else chunk,
                        "pdf_name": "Context Override",
                        "chunk_index": i,
                        "similarity_score": 1.0
                    })
            else:
                # Buscar conteúdo similar no ChromaDB em todos os PDFs do usuário
                similar_docs = self.chromadb.search_similar_content(
                    query=question,
                    pdf_name=None,  # Busca em todos os PDFs
                    user_id=user_id,
                    max_results=5
                )
                
                if not similar_docs:
                    error_msg = "Não encontrei informações relevantes nos seus documentos para responder essa pergunta."
                    logger.warning(error_msg)
                    return {
                        "question": question,
                        "answer": error_msg,
                        "error": "Nenhum conteúdo relevante encontrado",
                        "sources": []
                    }
                
                # Preparar contexto para o LLM
                context_texts = []
                sources = []
                
                for doc in similar_docs:
                    context_texts.append(doc["text"])
                    metadata = doc.get("metadata", {})
                    sources.append({
                        "text": doc["text"][:200] + "..." if len(doc["text"]) > 200 else doc["text"],
                        "pdf_name": metadata.get("pdf_name", "Unknown"),
                        "chunk_index": metadata.get("chunk_index", 0),
                        "similarity_score": doc.get("score", 0)
                    })
                
                combined_context = "\n\n".join(context_texts)
            
            # Criar prompt melhorado para pergunta geral
            enhanced_prompt = f"""
            Com base no seguinte contexto extraído dos documentos do usuário, responda à pergunta de forma clara e detalhada.
            
            CONTEXTO DOS DOCUMENTOS:
            {combined_context}
            
            PERGUNTA: {question}
            
            INSTRUÇÕES:
            - Use principalmente as informações do contexto fornecido
            - Se a informação não estiver no contexto, seja claro sobre isso e somente responda que não tem conhecimento sobre o assunto
            - Cite os documentos relevantes quando possível
            - Seja conversacional e útil
            - Não responder perguntas fora do contexto ou que não tenham informações relevantes nos documentos
            - Não inventar informações, apenas responder com base no contexto fornecido
            - Responda de forma direta e objetiva
            - Não incluir informações irrelevantes ou inventadas
            - Senão encontrar informações nos documentos, informe que você não tem conhecimento sobre o assunto
            - Quando não houver informações relevantes, informe que não encontrou dados suficientes para responder
            - Se perguntar sobre tabelas delta, delta lake, informe que deve procurar o time de inteligência de dados para orientação
            RESPOSTA:
            """
            
            # Gerar resposta usando o modelo LLM
            try:
                response = self.llm.invoke(enhanced_prompt)
                # Extrair conteúdo baseado no tipo de modelo
                if self.use_bedrock and hasattr(response, 'content'):
                    answer = response.content
                elif isinstance(response, str):
                    answer = response
                else:
                    answer = str(response)
            except Exception as e:
                logger.error(f"Erro ao gerar resposta com LLM: {e}")
                answer = f"Erro ao processar a pergunta. Contexto encontrado mas falha na geração da resposta: {e}"
            
            # Salvar interação no DynamoDB
            chat_id = None
            processing_time = time.time() - start_time
            
            # Determinar modelo usado
            model_used = "bedrock-claude-3.5-sonnet" if self.use_bedrock and self.bedrock_llm else "google-gemini-2.5-flash"
            
            if user_id:
                try:
                    chat_id = self.dynamodb.save_chat_interaction(
                        user_id=user_id,
                        pdf_name="general_chat",  # Marcador para chat geral
                        question=question,
                        answer=answer,
                        metadata={
                            "num_sources": len(sources),
                            "chromadb_collection": self.chromadb.default_collection,
                            "model_used": model_used,
                            "chat_type": "general",
                            "processing_time_seconds": Decimal(str(round(processing_time, 3))),
                            "processing_time_ms": Decimal(str(round(processing_time * 1000, 1)))
                        }
                    )
                except Exception as e:
                    logger.error(f"Erro ao salvar interação no DynamoDB: {e}")
            
            result = {
                "question": question,
                "answer": answer,
                "sources": sources,
                "metadata": {
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "num_sources_found": len(sources),
                    "total_context_length": len(combined_context),
                    "chat_type": "general",
                    "model_used": model_used,
                    "processing_time_seconds": round(processing_time, 3),
                    "processing_time_ms": round(processing_time * 1000, 1)
                }
            }
            
            logger.info(f"Pergunta geral processada com sucesso. Fontes: {len(sources)}, Tempo: {processing_time:.3f}s")
            return result
            
        except Exception as e:
            error_msg = f"Erro ao processar pergunta geral: {e}"
            logger.error(error_msg)
            return {
                "question": question,
                "answer": error_msg,
                "error": str(e),
                "sources": []
            }