"""
Versão simplificada do DynamoDBService para corrigir problemas de inicialização
"""
import boto3
from boto3.dynamodb.conditions import Key
from typing import Dict, List, Optional, Any
import uuid
from datetime import datetime
import logging
from botocore.exceptions import ClientError
import os

logger = logging.getLogger(__name__)

class DynamoDBService:
    """Serviço simplificado para DynamoDB"""
    
    def __init__(self):
        """Inicializa o serviço DynamoDB com tratamento robusto de erros"""
        try:
            # Configurar cliente DynamoDB
            self.region = os.getenv('AWS_REGION', 'ca-central-1')
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            
            # Nomes das tabelas
            self.tables = {
                'users': os.getenv('DYNAMODB_TABLE_USERS', 'chathib-users-stage'),
                'chat_history': os.getenv('DYNAMODB_TABLE_CHAT_HISTORY', 'chathib-chat-history-stage'),
                'pdfs': os.getenv('DYNAMODB_TABLE_PDFS', 'chathib_pdfs-stage')
            }
            
            # Verificar e criar tabelas se necessário
            self._ensure_tables_exist()
            
            self.available = True
            logger.info(f"DynamoDB inicializado. Região: {self.region}")
            
        except Exception as e:
            logger.warning(f"Erro ao inicializar DynamoDB: {e}")
            self.dynamodb = None
            self.available = False
    
    def _ensure_tables_exist(self):
        """Verifica se as tabelas existem e as cria se necessário"""
        if not self.dynamodb:
            return
        
        # Definições das tabelas
        table_definitions = {
            'users': {
                'KeySchema': [
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'user_id', 'AttributeType': 'S'}
                ]
            },
            'chat_history': {
                'KeySchema': [
                    {'AttributeName': 'chat_id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'chat_id', 'AttributeType': 'S'},
                    {'AttributeName': 'user_id', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                ],
                'GlobalSecondaryIndexes': [
                    {
                        'IndexName': 'user_id-index',
                        'KeySchema': [
                            {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            },
            'pdfs': {
                'KeySchema': [
                    {'AttributeName': 'pdf_id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pdf_id', 'AttributeType': 'S'},
                    {'AttributeName': 'user_id', 'AttributeType': 'S'}
                ],
                'GlobalSecondaryIndexes': [
                    {
                        'IndexName': 'user_id-index',
                        'KeySchema': [
                            {'AttributeName': 'user_id', 'KeyType': 'HASH'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            }
        }
        
        for table_key, table_name in self.tables.items():
            try:
                # Verificar se a tabela existe
                table = self.dynamodb.Table(table_name)
                table.load()
                logger.info(f"Tabela '{table_name}' já existe")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # Tabela não existe, criar
                    logger.info(f"Criando tabela '{table_name}'...")
                    self._create_table(table_name, table_definitions[table_key])
                else:
                    logger.error(f"Erro ao verificar tabela '{table_name}': {e}")
            except Exception as e:
                logger.error(f"Erro inesperado ao verificar tabela '{table_name}': {e}")
    
    def _create_table(self, table_name: str, table_definition: dict):
        """Cria uma tabela no DynamoDB"""
        try:
            table_params = {
                'TableName': table_name,
                'KeySchema': table_definition['KeySchema'],
                'AttributeDefinitions': table_definition['AttributeDefinitions'],
                'BillingMode': 'PAY_PER_REQUEST'
            }
            
            # Adicionar GSI se existir
            if 'GlobalSecondaryIndexes' in table_definition:
                table_params['GlobalSecondaryIndexes'] = table_definition['GlobalSecondaryIndexes']
            
            # Criar tabela
            table = self.dynamodb.create_table(**table_params)
            
            # Aguardar criação
            logger.info(f"Aguardando criação da tabela '{table_name}'...")
            table.wait_until_exists()
            
            logger.info(f"Tabela '{table_name}' criada com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao criar tabela '{table_name}': {e}")
            raise e
    
    def is_available(self) -> bool:
        """Verifica se DynamoDB está disponível"""
        is_avail = self.available and self.dynamodb is not None
        logger.info(f"DynamoDB availability check: available={self.available}, dynamodb={self.dynamodb is not None}, result={is_avail}")
        return is_avail
    
    def create_user(self, user_data: Dict[str, Any]) -> str:
        """Cria um novo usuário ou atualiza existente baseado no email"""
        if not self.is_available():
            logger.warning("DynamoDB não disponível - simulando criação de usuário")
            return str(uuid.uuid4())
        
        try:
            email = user_data.get('email', '')
            
            # Primeiro, tenta buscar usuário existente por email
            existing_user = self.get_user_by_email(email)
            
            if existing_user:
                # Usuário já existe, atualiza informações
                user_id = existing_user['user_id']
                item = {
                    'user_id': user_id,
                    'name': user_data.get('name', existing_user.get('name', 'Unknown')),
                    'email': email,
                    'created_at': existing_user.get('created_at', datetime.now().isoformat()),
                    'updated_at': datetime.now().isoformat(),
                    'is_active': True
                }
                
                # Adiciona informações adicionais se fornecidas
                if 'additional_info' in user_data:
                    item.update(user_data['additional_info'])
                
                logger.info(f"Atualizando usuário existente: {user_id}")
            else:
                # Usuário não existe, cria novo
                user_id = str(uuid.uuid4())
                item = {
                    'user_id': user_id,
                    'name': user_data.get('name', 'Unknown'),
                    'email': email,
                    'created_at': datetime.now().isoformat(),
                    'is_active': True
                }
                
                # Adiciona informações adicionais se fornecidas
                if 'additional_info' in user_data:
                    item.update(user_data['additional_info'])
                
                logger.info(f"Criando novo usuário: {user_id}")
            
            table = self.dynamodb.Table(self.tables['users'])
            table.put_item(Item=item)
            
            return user_id
            
        except Exception as e:
            logger.error(f"Erro ao criar/atualizar usuário: {e}")
            # Retornar ID mesmo com erro para não quebrar a aplicação
            return str(uuid.uuid4())
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Busca usuário por email (scan - não otimizado, mas funciona)"""
        if not self.is_available():
            return None
        
        try:
            table = self.dynamodb.Table(self.tables['users'])
            response = table.scan(
                FilterExpression='email = :email',
                ExpressionAttributeValues={':email': email}
            )
            
            items = response.get('Items', [])
            return items[0] if items else None
            
        except Exception as e:
            logger.error(f"Erro ao buscar usuário por email: {e}")
            return None
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        """Obtém usuário por ID"""
        if not self.is_available():
            return None
        
        try:
            table = self.dynamodb.Table(self.tables['users'])
            response = table.get_item(Key={'user_id': user_id})
            return response.get('Item')
        except Exception as e:
            logger.error(f"Erro ao obter usuário: {e}")
            return None
    
    def save_chat_interaction(self, user_id: str, pdf_name: str, question: str, 
                            answer: str, metadata: Dict = None) -> str:
        """Salva interação de chat"""
        if not self.is_available():
            logger.warning("DynamoDB não disponível - simulando salvamento de chat")
            return str(uuid.uuid4())
        
        try:
            chat_id = str(uuid.uuid4())
            timestamp = datetime.now().isoformat()
            
            item = {
                'chat_id': chat_id,
                'user_id': user_id,
                'pdf_name': pdf_name,
                'question': question,
                'answer': answer,
                'timestamp': timestamp,
                'metadata': metadata or {}
            }
            
            table = self.dynamodb.Table(self.tables['chat_history'])
            
            logger.info(f"  Salvando chat no DynamoDB:")
            logger.info(f"   - chat_id: {chat_id}")
            logger.info(f"   - user_id: {user_id}")
            logger.info(f"   - pdf_name: {pdf_name}")
            logger.info(f"   - question: {question[:100]}...")
            logger.info(f"   - answer: {answer[:100]}...")
            logger.info(f"   - timestamp: {timestamp}")
            logger.info(f"   - table: {self.tables['chat_history']}")
            
            table.put_item(Item=item)
            
            logger.info(f"Chat salvo com sucesso no DynamoDB: chat_id={chat_id}")
            return chat_id
            
        except Exception as e:
            logger.error(f"Erro ao salvar chat no DynamoDB: {e}")
            logger.error(f"   - user_id: {user_id}")
            logger.error(f"   - pdf_name: {pdf_name}")
            logger.error(f"   - question: {question[:100]}...")
            return str(uuid.uuid4())
    
    def get_recent_chats(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Obtém chats recentes do usuário"""
        if not self.is_available():
            return []
        
        try:
            table = self.dynamodb.Table(self.tables['chat_history'])
            # Usar GSI para query eficiente por user_id
            response = table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id),
                Limit=limit,
                ScanIndexForward=False  # Ordem decrescente (mais recentes primeiro)
            )
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Erro ao obter chats: {e}")
            return []
    
    def get_chat_history_by_pdf(self, user_id: str, pdf_name: str) -> List[Dict]:
        """Obtém histórico de chat por PDF"""
        if not self.is_available():
            return []
        
        try:
            table = self.dynamodb.Table(self.tables['chat_history'])
            # Usar GSI e filtrar por PDF
            response = table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id),
                FilterExpression=Key('pdf_name').eq(pdf_name)
            )
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Erro ao obter histórico por PDF: {e}")
            return []
    
    def save_pdf_metadata(self, user_id: str, pdf_name: str, metadata: Dict, processing_time_seconds: float = None) -> str:
        """Salva metadados de PDF incluindo tempo de processamento"""
        if not self.is_available():
            logger.warning("DynamoDB não disponível - simulando salvamento de PDF")
            return str(uuid.uuid4())
        
        try:
            from decimal import Decimal
            
            pdf_id = str(uuid.uuid4())
            item = {
                'pdf_id': pdf_id,
                'user_id': user_id,
                'pdf_name': pdf_name,
                'metadata': metadata,
                'created_at': datetime.now().isoformat()
            }
            
            # Adiciona tempo de processamento se fornecido
            if processing_time_seconds is not None:
                # Converter float para Decimal (requerido pelo DynamoDB)
                processing_time_decimal = Decimal(str(processing_time_seconds))
                item['processing_time_seconds'] = processing_time_decimal
                item['processing_time_formatted'] = self._format_processing_time(processing_time_seconds)
                logger.info(f"⏱Incluindo tempo de processamento: {processing_time_seconds}s -> {item['processing_time_formatted']}")
            
            table = self.dynamodb.Table(self.tables['pdfs'])
            
            print(f" DEBUG: Salvando item no DynamoDB:")
            print(f"   - Tabela: {self.tables['pdfs']}")
            print(f"   - pdf_id: {pdf_id}")
            print(f"   - pdf_name: {pdf_name}")
            print(f"   - user_id: {user_id}")
            if processing_time_seconds:
                print(f"   - processing_time_seconds: {processing_time_seconds}")
                print(f"   - processing_time_formatted: {item.get('processing_time_formatted', 'N/A')}")
            
            table.put_item(Item=item)
            print(f" DEBUG: Item salvo com sucesso")
            
            # Verificar se realmente foi salvo
            saved_item = self.get_pdf_by_id(pdf_id)
            if saved_item:
                print(f" DEBUG: Verificação pós-save bem-sucedida")
                print(f"   - PDF name: {saved_item.get('pdf_name', 'N/A')}")
                print(f"   - Has processing_time: {'processing_time_seconds' in saved_item}")
                if 'processing_time_seconds' in saved_item:
                    print(f"   - Processing time: {saved_item['processing_time_seconds']}s")
            else:
                print(f"DEBUG: Verificação pós-save falhou - PDF não encontrado")
            
            # Log detalhado do que foi salvo
            logger.info(f"PDF metadata salvo no DynamoDB:")
            logger.info(f"   - pdf_id: {pdf_id}")
            logger.info(f"   - pdf_name: {pdf_name}")
            logger.info(f"   - user_id: {user_id}")
            if processing_time_seconds:
                logger.info(f"   - processing_time_seconds: {processing_time_seconds}")
                logger.info(f"   - processing_time_formatted: {item.get('processing_time_formatted', 'N/A')}")
                logger.info(f"PDF salvo COM tempo de processamento: {pdf_id}")
            else:
                logger.info(f"PDF salvo SEM tempo de processamento: {pdf_id}")
            
            return pdf_id
            
        except Exception as e:
            logger.error(f"Erro ao salvar PDF metadata: {e}")
            return str(uuid.uuid4())
    
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
    
    def update_pdf_processing_time(self, pdf_id: str, processing_time_seconds: float) -> bool:
        """Atualiza apenas o tempo de processamento de um PDF existente"""
        if not self.is_available():
            logger.warning("DynamoDB não disponível - simulando update de tempo")
            return False
        
        try:
            from decimal import Decimal
            
            print(f"DEBUG: Iniciando update de tempo para PDF: {pdf_id}")
            print(f"DEBUG: Tempo: {processing_time_seconds}s -> {self._format_processing_time(processing_time_seconds)}")
            
            # Primeiro verificar se o PDF existe
            existing_pdf = self.get_pdf_by_id(pdf_id)
            if not existing_pdf:
                print(f"DEBUG: PDF {pdf_id} não encontrado para atualização")
                logger.error(f"PDF {pdf_id} não encontrado para atualização de tempo")
                return False
            
            print(f"DEBUG: PDF encontrado: {existing_pdf.get('pdf_name', 'N/A')}")
            
            # Converter float para Decimal (requerido pelo DynamoDB)
            processing_time_decimal = Decimal(str(processing_time_seconds))
            print(f"DEBUG: Convertido para Decimal: {processing_time_decimal}")
            
            table = self.dynamodb.Table(self.tables['pdfs'])
            
            response = table.update_item(
                Key={'pdf_id': pdf_id},
                UpdateExpression='SET processing_time_seconds = :pts, processing_time_formatted = :ptf',
                ExpressionAttributeValues={
                    ':pts': processing_time_decimal,
                    ':ptf': self._format_processing_time(processing_time_seconds)
                },
                ReturnValues='UPDATED_NEW'
            )
            
            print(f"DEBUG: Update response: {response}")
            
            # Verificar se realmente foi atualizado
            updated_pdf = self.get_pdf_by_id(pdf_id)
            if updated_pdf and 'processing_time_seconds' in updated_pdf:
                saved_time = updated_pdf['processing_time_seconds']
                # Converter de volta para float para comparação (DynamoDB retorna Decimal)
                if hasattr(saved_time, '__float__'):  # É um Decimal
                    saved_time_float = float(saved_time)
                else:
                    saved_time_float = saved_time
                    
                if isinstance(saved_time_float, (int, float)) and abs(saved_time_float - processing_time_seconds) < 0.01:
                    print(f"DEBUG: Tempo salvo corretamente: {saved_time}s")
                    logger.info(f"Tempo de processamento atualizado no DynamoDB: {pdf_id} -> {self._format_processing_time(processing_time_seconds)}")
                    return True
                else:
                    print(f"DEBUG: Tempo não salvo corretamente. Esperado: {processing_time_seconds}, Obtido: {saved_time_float}")
                    return False
            else:
                print(f"DEBUG: Tempo não encontrado no PDF após update")
                return False
            
        except Exception as e:
            print(f"DEBUG: Erro detalhado no update: {e}")
            logger.error(f"Erro ao atualizar tempo de processamento: {e}")
            return False
    
    def get_pdf_by_id(self, pdf_id: str) -> Optional[Dict]:
        """Busca um PDF por ID para verificar se existe"""
        if not self.is_available():
            return None
        
        try:
            table = self.dynamodb.Table(self.tables['pdfs'])
            response = table.get_item(Key={'pdf_id': pdf_id})
            return response.get('Item')
        except Exception as e:
            logger.error(f"Erro ao buscar PDF por ID: {e}")
            return None
    
    def verify_pdf_processing_time(self, pdf_id: str) -> Dict[str, Any]:
        """Verifica se um PDF tem tempo de processamento salvo"""
        try:
            pdf_data = self.get_pdf_by_id(pdf_id)
            if not pdf_data:
                return {
                    'found': False,
                    'error': f'PDF {pdf_id} não encontrado'
                }
            
            has_time = 'processing_time_seconds' in pdf_data
            has_formatted = 'processing_time_formatted' in pdf_data
            
            result = {
                'found': True,
                'pdf_name': pdf_data.get('pdf_name', 'N/A'),
                'has_processing_time': has_time,
                'has_formatted_time': has_formatted,
                'processing_time_seconds': pdf_data.get('processing_time_seconds'),
                'processing_time_formatted': pdf_data.get('processing_time_formatted'),
                'created_at': pdf_data.get('created_at')
            }
            
            return result
            
        except Exception as e:
            return {
                'found': False,
                'error': f'Erro ao verificar PDF: {e}'
            }
    
    def get_user_pdfs(self, user_id: str) -> List[Dict]:
        """Obtém PDFs do usuário"""
        if not self.is_available():
            return []
        
        try:
            table = self.dynamodb.Table(self.tables['pdfs'])
            # Usar GSI para query eficiente por user_id
            response = table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Erro ao obter PDFs do usuário: {e}")
            return []

    def get_full_pdfs(self, limit: int = 10) -> List[Dict]:
        """Obtém os últimos PDFs enviados ordenados por created_at"""
        if not self.is_available():
            return []
        
        try:
            table = self.dynamodb.Table(self.tables['pdfs'])
            
            # Usar scan para obter todos os PDFs
            response = table.scan(
                Limit=limit
            )
            
            pdfs = response.get('Items', [])
            
            # Ordenar por created_at em ordem decrescente (mais recentes primeiro)
            if pdfs:
                pdfs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            return pdfs[:limit]
            
        except Exception as e:
            logger.error(f"Erro ao obter PDFs completos: {e}")
            return []

    def save_feedback(self, chat_id: str, feedback_type: int, feedback_comment: str = "") -> bool:
        """Atualiza registro de chat existente com feedback (0=positivo, 1=negativo)"""
        if not self.is_available():
            logger.warning("DynamoDB não disponível - simulando salvamento de feedback")
            return False
        
        try:
            table = self.dynamodb.Table(self.tables['chat_history'])
            
            # Fazer update do registro existente
            response = table.update_item(
                Key={'chat_id': chat_id},
                UpdateExpression='SET feedback_date = :fd, feedback_type = :ft, feedback_comment = :fc',
                ExpressionAttributeValues={
                    ':fd': datetime.now().isoformat(),
                    ':ft': feedback_type,
                    ':fc': feedback_comment
                },
                ReturnValues='UPDATED_NEW'
            )
            
            feedback_text = "positivo" if feedback_type == 0 else "negativo"
            logger.info(f"Feedback atualizado - Chat ID: {chat_id}, Type: {feedback_text} ({feedback_type}), Comment: {feedback_comment[:50]}...")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao atualizar feedback para chat {chat_id}: {e}")
            return False
    
    def get_user_feedback(self, user_id: str, limit: int = 50) -> List[Dict]:
        """Obtém histórico de feedback do usuário (chats que têm feedback)"""
        if not self.is_available():
            return []
        
        try:
            table = self.dynamodb.Table(self.tables['chat_history'])
            
            # Query por user_id e filtra por feedback_date (indica que tem feedback)
            response = table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id),
                FilterExpression='attribute_exists(feedback_date)',
                Limit=limit,
                ScanIndexForward=False  # Ordem decrescente (mais recentes primeiro)
            )
            
            feedbacks = response.get('Items', [])
            
            # Formatar dados para compatibilidade
            formatted_feedbacks = []
            for feedback in feedbacks:
                feedback_text = "positivo" if feedback.get('feedback_type') == 0 else "negativo"
                formatted_feedbacks.append({
                    'chat_id': feedback.get('chat_id'),
                    'question': feedback.get('question'),
                    'answer': feedback.get('answer'),
                    'feedback_type': feedback.get('feedback_type'),
                    'feedback_text': feedback_text,
                    'feedback_comment': feedback.get('feedback_comment', ''),
                    'feedback_date': feedback.get('feedback_date'),
                    'timestamp': feedback.get('timestamp')
                })
            
            return formatted_feedbacks
            
        except Exception as e:
            logger.error(f"Erro ao obter feedback do usuário: {e}")
            return []
    
    def get_chat_history(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Obtém histórico de chat do usuário ordenado por timestamp"""
        if not self.is_available():
            logger.warning("DynamoDB não disponível")
            return []
        
        try:
            table = self.dynamodb.Table(self.tables['chat_history'])
            table_name = self.tables['chat_history']
            
            logger.info(f"DynamoDB: Buscando chat history para user_id: {user_id}, limit: {limit}")
            logger.info(f"DynamoDB: Tabela: {table_name}")
            
            # Primeiro, verificar se o GSI existe
            try:
                table_description = table.meta.client.describe_table(TableName=table_name)
                gsi_exists = any(
                    gsi.get('IndexName') == 'user_id-index' 
                    for gsi in table_description.get('Table', {}).get('GlobalSecondaryIndexes', [])
                )
                logger.info(f"DynamoDB: GSI 'user_id-index' existe: {gsi_exists}")
            except Exception as gsi_error:
                logger.error(f"DynamoDB: Erro ao verificar GSI: {gsi_error}")
                gsi_exists = False
            
            if gsi_exists:
                # Usar GSI para query eficiente por user_id
                response = table.query(
                    IndexName='user_id-index',
                    KeyConditionExpression=Key('user_id').eq(user_id),
                    Limit=limit,
                    ScanIndexForward=False  # Ordem decrescente (mais recentes primeiro)
                )
                chats = response.get('Items', [])
                logger.info(f"DynamoDB: Query por GSI retornou {len(chats)} chats")
            else:
                # Fallback: usar scan com filtro (menos eficiente)
                logger.warning("DynamoDB: GSI não encontrado, usando scan")
                response = table.scan(
                    FilterExpression=Key('user_id').eq(user_id),
                    Limit=limit
                )
                chats = response.get('Items', [])
                logger.info(f"DynamoDB: Scan retornou {len(chats)} chats")
            
            # Ordenar por timestamp se disponível
            if chats:
                chats.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
                logger.info(f"DynamoDB: Chats ordenados por timestamp")

            # Inverter a lista para que o registro mais recente fique no final
            chats.reverse()
            logger.info(f"DynamoDB: Lista invertida - registro mais recente no final")
            
            # Log dos primeiros chats encontrados
            if chats:
                logger.info(f"DynamoDB: Exemplo de chat encontrado:")
                first_chat = chats[0]
                logger.info(f"   - chat_id: {first_chat.get('chat_id')}")
                logger.info(f"   - user_id: {first_chat.get('user_id')}")
                logger.info(f"   - timestamp: {first_chat.get('timestamp')}")
                logger.info(f"   - question: {first_chat.get('question', '')[:50]}...")
            
            return chats
            
        except Exception as e:
            logger.error(f"DynamoDB: Erro ao obter chat history: {e}")
            logger.error(f"   - user_id: {user_id}")
            logger.error(f"   - limit: {limit}")
            logger.error(f"   - table: {self.tables['chat_history']}")
            return []
