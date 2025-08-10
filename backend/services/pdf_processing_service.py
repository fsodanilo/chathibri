import os
import tempfile
import logging
from typing import List, Dict, Any, Optional, Tuple
import PyPDF2
from services.chromadb_client import ChromaDBService
from services.dynamodb_service import DynamoDBService
import uuid
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class PDFProcessingService:
    """Serviço para processamento de PDFs com armazenamento em ChromaDB e DynamoDB"""
    
    def __init__(self):
        """Inicializa o serviço de processamento de PDF"""
        self.chromadb = ChromaDBService()
        self.dynamodb = DynamoDBService()
        
        # Configurações de chunking
        self.chunk_size = 1000  # Tamanho base dos chunks em caracteres
        self.chunk_overlap = 200  # Sobreposição entre chunks
        self.min_chunk_size = 100  # Tamanho mínimo do chunk
    
    def extract_text_from_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Extrai texto de um PDF usando PyPDF2
        
        Args:
            pdf_path: Caminho para o arquivo PDF
        
        Returns:
            Dicionário com texto extraído e metadados
        """
        try:
            print(f"DEBUG: Abrindo arquivo PDF: {pdf_path}")
            with open(pdf_path, 'rb') as file:
                print(f"DEBUG: Criando PdfReader")
                pdf_reader = PyPDF2.PdfReader(file)
                print(f"DEBUG: PDF tem {len(pdf_reader.pages)} páginas")
                
                text = ""
                pages_info = []
                
                for page_num, page in enumerate(pdf_reader.pages):
                    print(f"DEBUG: Processando página {page_num + 1}")
                    try:
                        page_text = page.extract_text()
                        text += page_text + "\n\n"
                        
                        pages_info.append({
                            'page_number': page_num + 1,
                            'text': page_text,
                            'char_count': len(page_text),
                            'word_count': len(page_text.split()) if page_text else 0
                        })
                        print(f"DEBUG: Página {page_num + 1} processada: {len(page_text)} chars")
                    except Exception as e:
                        print(f"DEBUG: Erro na página {page_num + 1}: {e}")
                        logger.warning(f"Erro ao extrair texto da página {page_num + 1}: {e}")
                        pages_info.append({
                            'page_number': page_num + 1,
                            'text': "",
                            'char_count': 0,
                            'word_count': 0,
                            'error': str(e)
                        })
                
                print(f"DEBUG: Extração concluída. Total: {len(text)} caracteres")
                return {
                    'success': True,
                    'full_text': text.strip(),
                    'pages': pages_info,
                    'page_count': len(pdf_reader.pages),
                    'total_chars': len(text),
                    'total_words': len(text.split()) if text else 0,
                    'extraction_method': 'PyPDF2'
                }
                
        except Exception as e:
            print(f"DEBUG: Erro em extract_text_from_pdf: {e}")
            logger.error(f"Erro ao extrair texto do PDF: {e}")
            return {
                'success': False,
                'error': str(e),
                'full_text': "",
                'pages': [],
                'page_count': 0,
                'total_chars': 0,
                'total_words': 0
            }
    
    def create_text_chunks(self, text: str, pdf_name: str = "") -> List[Dict[str, Any]]:
        """
        Divide texto em chunks inteligentes
        
        Args:
            text: Texto completo para dividir
            pdf_name: Nome do PDF (para metadados)
        
        Returns:
            Lista de chunks com metadados
        """
        try:
            print(f"DEBUG: Iniciando create_text_chunks para {pdf_name}, texto: {len(text)} chars")
            
            # Limpar e normalizar texto
            print(f"DEBUG: Limpando texto...")
            text = self._clean_text(text)
            print(f"DEBUG: Texto limpo: {len(text)} chars")
            
            if len(text) < self.min_chunk_size:
                print(f"DEBUG: Texto menor que min_chunk_size ({self.min_chunk_size}), criando chunk único")
                return [{
                    'text': text,
                    'chunk_index': 0,
                    'start_char': 0,
                    'end_char': len(text),
                    'char_count': len(text),
                    'word_count': len(text.split()),
                    'pdf_name': pdf_name
                }]
            
            print(f"DEBUG: Iniciando loop de chunking...")
            chunks = []
            chunk_index = 0
            start = 0
            max_iterations = len(text) // 100 + 100  # Proteção contra loop infinito
            iteration_count = 0
            
            while start < len(text) and iteration_count < max_iterations:
                iteration_count += 1
                print(f"DEBUG: Chunk {chunk_index}, start: {start}, iteration: {iteration_count}")
                
                if iteration_count % 10 == 0:  # Log a cada 10 iterações
                    print(f"DEBUG: Progresso chunking: {iteration_count}/{max_iterations}, start: {start}/{len(text)}")
                
                # Calcular fim do chunk
                end = start + self.chunk_size
                
                # Se não é o último chunk, tentar encontrar uma quebra natural
                if end < len(text):
                    print(f"DEBUG: Procurando quebra natural...")
                    try:
                        end = self._find_natural_break(text, start, end)
                        print(f"DEBUG: Quebra natural encontrada em: {end}")
                    except Exception as e:
                        print(f"DEBUG: Erro ao procurar quebra natural: {e}, usando end original")
                        end = start + self.chunk_size
                else:
                    end = len(text)
                    print(f"DEBUG: Último chunk, end: {end}")
                
                # Extrair chunk
                chunk_text = text[start:end].strip()
                print(f"DEBUG: Chunk extraído: {len(chunk_text)} chars")
                
                if len(chunk_text) >= self.min_chunk_size:
                    chunk_info = {
                        'text': chunk_text,
                        'chunk_index': chunk_index,
                        'start_char': start,
                        'end_char': end,
                        'char_count': len(chunk_text),
                        'word_count': len(chunk_text.split()),
                        'pdf_name': pdf_name
                    }
                    chunks.append(chunk_info)
                    chunk_index += 1
                    print(f"DEBUG: Chunk {chunk_index-1} adicionado")
                
                # Avançar com sobreposição
                old_start = start
                start = end - self.chunk_overlap
                if start < 0:
                    start = 0
                
                # Proteção contra loop infinito - se start não avançou suficientemente
                if start <= old_start:
                    print(f"DEBUG: Start não avançou suficientemente ({old_start} -> {start}), forçando avanço")
                    start = old_start + max(1, self.chunk_size // 2)
                
                print(f"DEBUG: Próximo start: {start}")
                
                # Se start >= end, algo deu errado
                if start >= end and end < len(text):
                    print(f"DEBUG: ERRO: start >= end, corrigindo...")
                    start = end
                    
            if iteration_count >= max_iterations:
                print(f"DEBUG: AVISO: Atingido limite de iterações ({max_iterations})")
                logger.warning(f"Chunking atingiu limite de iterações para PDF '{pdf_name}'")
            
            print(f"DEBUG: Chunking concluído: {len(chunks)} chunks criados em {iteration_count} iterações")
            logger.info(f"Texto dividido em {len(chunks)} chunks para PDF '{pdf_name}'")
            return chunks
            
        except Exception as e:
            print(f"DEBUG: Erro em create_text_chunks: {e}")
            logger.error(f"Erro ao criar chunks: {e}")
            return []
    
    def _clean_text(self, text: str) -> str:
        """Limpa e normaliza texto"""
        # Remover caracteres de controle e normalizar espaços
        text = re.sub(r'\s+', ' ', text)  # Múltiplos espaços -> espaço único
        text = re.sub(r'\n\s*\n', '\n\n', text)  # Múltiplas quebras -> dupla quebra
        text = text.strip()
        return text
    
    def _find_natural_break(self, text: str, start: int, preferred_end: int) -> int:
        """Encontra um ponto natural para quebrar o texto"""
        try:
            print(f"DEBUG: _find_natural_break - start: {start}, preferred_end: {preferred_end}, text_len: {len(text)}")
            
            # Validar entrada
            if preferred_end >= len(text):
                print(f"DEBUG: preferred_end >= text_len, retornando len(text)")
                return len(text)
            
            if preferred_end <= start:
                print(f"DEBUG: preferred_end <= start, retornando preferred_end")
                return preferred_end
            
            # Procurar por quebras naturais em uma janela menor para evitar travamento
            search_window = min(50, (preferred_end - start) // 2)  # Janela menor e limitada
            search_start = max(preferred_end - search_window, start)
            search_end = min(preferred_end + search_window, len(text))
            
            print(f"DEBUG: search_window: {search_window}, search_start: {search_start}, search_end: {search_end}")
            
            # Busca simples sem regex complexo para evitar travamento
            search_text = text[search_start:search_end]
            
            # Busca direta por caracteres, sem regex
            break_chars = ['\n\n', '. ', '.\n', '! ', '!\n', '? ', '?\n', '; ', ';\n', ': ', ':\n', ', ', ',\n']
            
            best_pos = None
            best_distance = float('inf')
            
            for break_char in break_chars:
                pos = search_text.find(break_char)
                if pos != -1:
                    absolute_pos = search_start + pos + len(break_char)
                    distance = abs(absolute_pos - preferred_end)
                    if distance < best_distance:
                        best_distance = distance
                        best_pos = absolute_pos
                        print(f"DEBUG: Encontrou quebra '{break_char}' em pos {absolute_pos}, distancia {distance}")
            
            if best_pos is not None:
                print(f"DEBUG: Melhor quebra em: {best_pos}")
                return best_pos
            
            # Se não encontrar quebra natural, procurar espaço mais próximo
            for i in range(preferred_end, search_start - 1, -1):
                if i < len(text) and text[i] == ' ':
                    print(f"DEBUG: Encontrou espaço em: {i}")
                    return i + 1
            
            # Último recurso: usar posição preferida
            print(f"DEBUG: Nenhuma quebra encontrada, usando preferred_end: {preferred_end}")
            return preferred_end
            
        except Exception as e:
            print(f"DEBUG: Erro em _find_natural_break: {e}")
            return preferred_end
    
    def process_pdf_file(self, pdf_path: str, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Processa um arquivo PDF completo: extração, chunking e armazenamento
        
        Args:
            pdf_path: Caminho para o arquivo PDF
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            Dicionário com resultado do processamento
        """
        try:
            print(f"DEBUG: Iniciando process_pdf_file para {pdf_name}")
            logger.info(f"Iniciando processamento do PDF: {pdf_name}")
            
            # 1. Extrair texto do PDF
            print(f"DEBUG: Iniciando extração de texto para {pdf_name}")
            extraction_result = self.extract_text_from_pdf(pdf_path)
            print(f"DEBUG: Extração concluída para {pdf_name}: {extraction_result.get('success', False)}")
            
            if not extraction_result['success']:
                print(f"DEBUG: Erro na extração: {extraction_result.get('error', 'Unknown')}")
                return {
                    'success': False,
                    'error': f"Erro na extração: {extraction_result['error']}",
                    'pdf_name': pdf_name
                }
            
            full_text = extraction_result['full_text']
            if not full_text.strip():
                print(f"DEBUG: PDF não contém texto extraível")
                return {
                    'success': False,
                    'error': "PDF não contém texto extraível",
                    'pdf_name': pdf_name
                }
            
            print(f"DEBUG: Texto extraído: {len(full_text)} caracteres")
            
            # 2. Criar chunks
            print(f"DEBUG: Iniciando criação de chunks para {pdf_name}")
            chunks = self.create_text_chunks(full_text, pdf_name)
            print(f"DEBUG: Chunks criados: {len(chunks)}")
            
            if not chunks:
                print(f"DEBUG: Não foi possível criar chunks")
                return {
                    'success': False,
                    'error': "Não foi possível criar chunks do texto",
                    'pdf_name': pdf_name
                }
            
            # 3. Armazenar embeddings no ChromaDB
            print(f"DEBUG: Iniciando armazenamento no ChromaDB para {pdf_name}")
            chunk_texts = [chunk['text'] for chunk in chunks]
            print(f"DEBUG: Preparados {len(chunk_texts)} textos de chunks para ChromaDB")
            
            pdf_metadata = {
                'page_count': extraction_result['page_count'],
                'total_chars': extraction_result['total_chars'],
                'total_words': extraction_result['total_words'],
                'chunks_count': len(chunks),
                'processing_date': datetime.utcnow().isoformat(),
                'extraction_method': extraction_result['extraction_method']
            }
            print(f"DEBUG: Metadados preparados: {pdf_metadata}")
            
            print(f"DEBUG: Chamando store_pdf_embeddings para {pdf_name}")
            try:
                chromadb_success = self.chromadb.store_pdf_embeddings(
                    pdf_name=pdf_name,
                    text_chunks=chunk_texts,
                    user_id=user_id,
                    pdf_metadata=pdf_metadata
                )
                print(f"DEBUG: ChromaDB storage result: {chromadb_success}")
            except Exception as chromadb_error:
                print(f"DEBUG: Erro durante store_pdf_embeddings: {chromadb_error}")
                logger.error(f"Erro específico no ChromaDB: {chromadb_error}")
                chromadb_success = False
            
            if not chromadb_success:
                print(f"DEBUG: Erro ao armazenar no ChromaDB")
                return {
                    'success': False,
                    'error': "Erro ao armazenar embeddings no ChromaDB",
                    'pdf_name': pdf_name
                }
            
            # 4. Salvar metadados no DynamoDB
            print(f"DEBUG: Iniciando salvamento no DynamoDB para {pdf_name}")
            dynamodb_metadata = {
                **pdf_metadata,
                'file_path': pdf_path,
                'chunks_info': chunks[:5],  # Salvar info dos primeiros 5 chunks
                'chromadb_indexed': True
            }
            
            pdf_id = None
            if user_id:
                try:
                    print(f"DEBUG: Salvando metadados no DynamoDB {user_id}")
                    # Salva sem tempo de processamento primeiro, será atualizado depois
                    pdf_id = self.dynamodb.save_pdf_metadata(user_id, pdf_name, dynamodb_metadata)
                    print(f"DEBUG: DynamoDB save result: {pdf_id}")
                except Exception as e:
                    print(f"DEBUG: Erro ao salvar no DynamoDB: {e}")
                    logger.warning(f"Erro ao salvar metadados no DynamoDB: {e}")
            
            # 5. Resultado final
            print(f"DEBUG: Preparando resultado final para {pdf_name}")
            result = {
                'success': True,
                'pdf_name': pdf_name,
                'pdf_id': pdf_id,
                'extraction_stats': {
                    'page_count': extraction_result['page_count'],
                    'total_chars': extraction_result['total_chars'],
                    'total_words': extraction_result['total_words'],
                    'chunks_created': len(chunks)
                },
                'storage_stats': {
                    'chromadb_indexed': chromadb_success,
                    'dynamodb_metadata_saved': pdf_id is not None,
                    'total_chunks_stored': len(chunks)
                },
                'processing_time': datetime.utcnow().isoformat()
            }
            
            print(f"DEBUG: Processamento completo para {pdf_name}")
            logger.info(f"PDF '{pdf_name}' processado com sucesso. {len(chunks)} chunks indexados.")
            return result
            
        except Exception as e:
            error_msg = f"Erro ao processar PDF '{pdf_name}': {e}"
            print(f"DEBUG: Exception em process_pdf_file: {error_msg}")
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'pdf_name': pdf_name
            }
    
    def process_uploaded_pdf(self, file_content: bytes, filename: str, user_id: str = None) -> Dict[str, Any]:
        """
        Processa um PDF enviado via upload
        
        Args:
            file_content: Conteúdo do arquivo em bytes
            filename: Nome do arquivo
            user_id: ID do usuário
        
        Returns:
            Resultado do processamento
        """
        import time
        start_time = time.time()
        
        print('*********************Processing uploaded PDF*******************:', filename)
        try:
            # Criar arquivo temporário
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_file.write(file_content)
                temp_path = temp_file.name
                print(f"Arquivo temporário criado: {temp_path}")
            try:
                # Processar o arquivo
                result = self.process_pdf_file(temp_path, filename, user_id)
                
                print(f"DEBUG: Resultado do process_pdf_file:")
                print(f"   - Success: {result.get('success', False)}")
                print(f"   - PDF ID: {result.get('pdf_id', 'N/A')}")
                print(f"   - PDF Name: {result.get('pdf_name', 'N/A')}")
                print(f"   - Keys disponíveis: {list(result.keys())}")
                
                # Calcular tempo de processamento
                processing_time = time.time() - start_time
                print(f"⏱DEBUG: Tempo calculado: {processing_time}s")
                
                # Atualizar resultado com tempo de processamento
                result['processing_time_seconds'] = processing_time
                result['processing_time_formatted'] = self._format_processing_time(processing_time)
                
                print(f"DEBUG: Verificando condições para update:")
                print(f"   - user_id fornecido: {user_id is not None} (valor: {user_id})")
                print(f"   - result tem pdf_id: {'pdf_id' in result}")
                print(f"   - pdf_id não é None: {result.get('pdf_id') is not None}")
                print(f"   - Condição geral: {user_id and result.get('pdf_id')}")
                
                # Agora atualizar o DynamoDB com tempo de processamento se PDF foi salvo
                if user_id and result.get('pdf_id'):
                    try:
                        pdf_id = result['pdf_id']
                        print(f"Atualizando PDF {pdf_id} com tempo: {processing_time}s")
                        
                        # Verificar PDF antes do update
                        verification_before = self.dynamodb.verify_pdf_processing_time(pdf_id)
                        print(f"PDF antes do update: {verification_before}")
                        
                        success = self.dynamodb.update_pdf_processing_time(pdf_id, processing_time)
                        
                        if success:
                            print(f"Tempo de processamento salvo no DynamoDB: {self._format_processing_time(processing_time)}")
                            
                            # Verificar PDF após o update
                            verification_after = self.dynamodb.verify_pdf_processing_time(pdf_id)
                            print(f"PDF após o update: {verification_after}")
                            
                        else:
                            print(f"Falha ao salvar tempo no DynamoDB")
                            
                    except Exception as update_error:
                        print(f"Erro ao atualizar tempo no DynamoDB: {update_error}")
                elif not user_id:
                    print(f"Não atualizando tempo: user_id não fornecido")
                elif not result.get('pdf_id'):
                    print(f"Não atualizando tempo: pdf_id não encontrado no resultado")
                    print(f"Resultado disponível: {list(result.keys())}")
                    print(f"Success status: {result.get('success', 'N/A')}")
                    if 'error' in result:
                        print(f"Error message: {result['error']}")
                else:
                    print(f"Condição não atendida por motivo desconhecido")
                    print(f"   - user_id: {user_id}")
                    print(f"   - pdf_id: {result.get('pdf_id')}")
                
                print(f"Resultado do processamento: {result}")
                print(f"Tempo de processamento: {result.get('processing_time_formatted', 'N/A')}")
                return result
            finally:
                # Limpar arquivo temporário
                print(f"Removendo arquivo temporário: {temp_path}")
                try:
                    os.unlink(temp_path)
                except:
                    pass
            print('*********************Processing uploaded PDF completed*******************:', filename)
        except Exception as e:
            error_msg = f"Erro ao processar upload do PDF '{filename}': {e}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'pdf_name': filename
            }
        
    def _format_processing_time(self, seconds: float) -> str:
        """Formata tempo de processamento em formato legível"""
        if seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.2f}min"
        else:
            hours = seconds / 3600
            return f"{hours:.2f}h"
    
    def reprocess_pdf(self, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Reprocessa um PDF existente (útil para atualizações de algoritmo)
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            Resultado do reprocessamento
        """
        try:
            # Primeiro, remover dados existentes
            self.delete_pdf_data(pdf_name, user_id)
            
            # TODO: Implementar lógica para reprocessar a partir do arquivo original
            # Por enquanto, retorna erro indicando que precisa de novo upload
            
            return {
                'success': False,
                'error': "Reprocessamento requer novo upload do arquivo",
                'pdf_name': pdf_name,
                'action_required': "upload_new_file"
            }
            
        except Exception as e:
            error_msg = f"Erro ao reprocessar PDF '{pdf_name}': {e}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'pdf_name': pdf_name
            }
    
    def delete_pdf_data(self, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Remove todos os dados de um PDF (ChromaDB e DynamoDB)
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            Resultado da operação
        """
        try:
            results = {
                'chromadb_deleted': False,
                'dynamodb_metadata_found': False,
                'errors': []
            }
            
            # Remover do ChromaDB
            try:
                chromadb_success = self.chromadb.delete_pdf_embeddings(pdf_name, user_id)
                results['chromadb_deleted'] = chromadb_success
                if not chromadb_success:
                    results['errors'].append("Erro ao remover do ChromaDB")
            except Exception as e:
                results['errors'].append(f"Erro ChromaDB: {e}")
            
            # TODO: Implementar remoção de metadados do DynamoDB
            # Por enquanto, apenas marcar que foi tentado
            
            success = results['chromadb_deleted'] and len(results['errors']) == 0
            
            result = {
                'success': success,
                'pdf_name': pdf_name,
                'details': results
            }
            
            if not success:
                result['error'] = "; ".join(results['errors'])
            
            logger.info(f"Dados do PDF '{pdf_name}' removidos. Sucesso: {success}")
            return result
            
        except Exception as e:
            error_msg = f"Erro ao deletar dados do PDF '{pdf_name}': {e}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'pdf_name': pdf_name
            }
    
    def get_pdf_processing_status(self, pdf_name: str, user_id: str = None) -> Dict[str, Any]:
        """
        Verifica o status de processamento de um PDF
        
        Args:
            pdf_name: Nome do PDF
            user_id: ID do usuário
        
        Returns:
            Status do processamento
        """
        try:
            # Verificar no ChromaDB
            chunks = self.chromadb.get_pdf_chunks(pdf_name, user_id)
            chromadb_indexed = len(chunks) > 0
            
            # Verificar metadados no DynamoDB
            user_pdfs = []
            if user_id:
                user_pdfs = self.dynamodb.get_user_pdfs(user_id)
            
            pdf_metadata = None
            for pdf_data in user_pdfs:
                if pdf_data.get("pdf_name") == pdf_name:
                    pdf_metadata = pdf_data.get("metadata", {})
                    break
            
            status = {
                'pdf_name': pdf_name,
                'chromadb_indexed': chromadb_indexed,
                'chunks_count': len(chunks),
                'has_metadata': pdf_metadata is not None,
                'metadata': pdf_metadata or {},
                'processing_complete': chromadb_indexed and pdf_metadata is not None
            }
            
            if chromadb_indexed:
                # Adicionar estatísticas dos chunks
                if chunks:
                    total_chars = sum(len(chunk.get("text", "")) for chunk in chunks)
                    status['chunks_stats'] = {
                        'total_characters': total_chars,
                        'avg_chunk_size': total_chars / len(chunks),
                        'min_chunk_size': min(len(chunk.get("text", "")) for chunk in chunks),
                        'max_chunk_size': max(len(chunk.get("text", "")) for chunk in chunks)
                    }
            
            return status
            
        except Exception as e:
            logger.error(f"Erro ao verificar status do PDF '{pdf_name}': {e}")
            return {
                'pdf_name': pdf_name,
                'error': str(e),
                'chromadb_indexed': False,
                'processing_complete': False
            }
    
    def list_processed_pdfs(self, user_id: str = None) -> List[Dict[str, Any]]:
        """
        Lista todos os PDFs processados
        
        Args:
            user_id: ID do usuário (opcional)
        
        Returns:
            Lista de PDFs processados com status
        """
        try:
            # Obter PDFs indexados no ChromaDB
            indexed_pdfs = self.chromadb.list_indexed_pdfs(user_id)
            
            # Obter metadados do DynamoDB
            metadata_pdfs = {}
            if user_id:
                user_pdfs = self.dynamodb.get_user_pdfs(user_id)
                metadata_pdfs = {pdf.get("pdf_name", ""): pdf for pdf in user_pdfs}
            
            # Combinar informações
            processed_pdfs = []
            
            for pdf_name in indexed_pdfs:
                pdf_status = self.get_pdf_processing_status(pdf_name, user_id)
                processed_pdfs.append(pdf_status)
            
            # Adicionar PDFs que estão apenas no DynamoDB
            for pdf_name, pdf_data in metadata_pdfs.items():
                if pdf_name not in indexed_pdfs:
                    pdf_status = {
                        'pdf_name': pdf_name,
                        'chromadb_indexed': False,
                        'chunks_count': 0,
                        'has_metadata': True,
                        'metadata': pdf_data.get("metadata", {}),
                        'processing_complete': False,
                        'status': 'metadata_only'
                    }
                    processed_pdfs.append(pdf_status)
            
            return processed_pdfs
            
        except Exception as e:
            logger.error(f"Erro ao listar PDFs processados: {e}")
            return []
