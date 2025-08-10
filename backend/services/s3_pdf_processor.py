import boto3
import pandas as pd
import os
import time
import json
import io
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import PyPDF2
import tempfile
import google.generativeai as genai
from botocore.exceptions import ClientError
import re
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
from langchain_aws import ChatBedrock

# Configurações do Athena
ATHENA_DATABASE = "chathib_stage"

class S3PDFProcessor:
    """Processador de PDF com extração de tabelas e salvamento no S3"""
    
    def __init__(self, use_bedrock: bool = True):
        """Inicializa o processador com configurações do S3 e IA"""
        self.use_bedrock = use_bedrock
        self.setup_s3_client()
        self.setup_ai_models()
        self.setup_glue_client()
        self.bucket_name = "dl-landing-zone-ca-central-1-stage"
        self.s3_folder = "chathib-prod"
        
    def setup_s3_client(self):
        """Configura cliente S3 com credenciais AWS"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_DEFAULT_REGION', 'ca-central-1')
            )
            print("Cliente S3 configurado com sucesso")
        except Exception as e:
            print(f"Erro ao configurar S3: {str(e)}")
            self.s3_client = None
    
    def setup_ai_models(self):
        """Configura modelos de IA (Bedrock e Google AI)"""
        self.bedrock_model = None
        self.google_model = None
        
        # Configura AWS Bedrock primeiro
        if self.use_bedrock:
            try:
                self.bedrock_model = ChatBedrock(
                    model_id="anthropic.claude-3-5-sonnet-20240620-v1:0",
                    model_kwargs={
                        "max_tokens": 4000,
                        "temperature": 0.1,
                        "top_p": 1,
                        "stop_sequences": [],
                    },
                    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
                )
                print("AWS Bedrock (Claude 3.5 Sonnet) configurado para extração inteligente")
            except Exception as e:
                print(f"Erro ao configurar Bedrock: {e}")
                self.bedrock_model = None
        
        # Configura Google AI
        GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
        if GOOGLE_API_KEY:
            try:
                genai.configure(api_key=GOOGLE_API_KEY)
                self.google_model = genai.GenerativeModel('gemini-2.5-flash-lite-preview-06-17')
                print("Google AI configurada como fallback para extração inteligente")
            except Exception as e:
                print(f"Erro ao configurar Google AI: {e}")
                self.google_model = None
        
        # Determina qual modelo usar
        if self.bedrock_model:
            self.model = self.bedrock_model
            self.model_type = "bedrock"
        elif self.google_model:
            self.model = self.google_model
            self.model_type = "google"
        else:
            print("Nenhum modelo de IA configurado. Extração inteligente desabilitada.")
            self.model = None
            self.model_type = None
    
    def setup_glue_client(self):
        """Configura cliente AWS Glue para catalog de tabelas"""
        try:
            self.glue_client = boto3.client(
                'glue',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_DEFAULT_REGION', 'ca-central-1')
            )
            print("Cliente AWS Glue configurado com sucesso")
        except Exception as e:
            print(f"Erro ao configurar Glue: {str(e)}")
            self.glue_client = None
    
    def extract_text_from_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """Extrai texto estruturado do PDF"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                # Verificar se o PDF está criptografado
                if pdf_reader.is_encrypted:
                    print(f"PDF está criptografado, tentando descriptografar...")
                    # Tentar descriptografar com senha vazia (muitos PDFs são protegidos apenas contra edição)
                    try:
                        pdf_reader.decrypt('')
                        print(f"DF descriptografado com sucesso")
                    except Exception as decrypt_error:
                        print(f"Falha na descriptografia: {str(decrypt_error)}")
                        return {
                            'error': f'PDF criptografado e não foi possível descriptografar: {str(decrypt_error)}',
                            'is_encrypted': True
                        }
                
                full_text = ""
                pages_info = []
                
                for page_num, page in enumerate(pdf_reader.pages):
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n\n"
                        pages_info.append({
                            'page': page_num + 1,
                            'text': text,
                            'char_count': len(text)
                        })
                
                return {
                    'full_text': full_text,
                    'pages': pages_info,
                    'page_count': len(pdf_reader.pages),
                    'total_chars': len(full_text),
                    'is_encrypted': False
                }
        except ImportError as ie:
            error_msg = f"Biblioteca necessária não encontrada: {str(ie)}"
            if "PyCryptodome" in str(ie) or "AES" in str(ie):
                error_msg = "PyCryptodome é necessário para processar PDFs criptografados. Execute: pip install PyCryptodome"
            print(f"Erro de dependência: {error_msg}")
            return {
                'error': error_msg,
                'is_dependency_error': True
            }
        except Exception as e:
            error_msg = str(e)
            if "PyCryptodome" in error_msg or "AES" in error_msg:
                error_msg = "PyCryptodome é necessário para processar este PDF criptografado. Execute: pip install PyCryptodome"
            print(f"Erro ao extrair texto: {error_msg}")
            return {
                'error': error_msg,
                'is_encrypted_error': "crypt" in error_msg.lower() or "aes" in error_msg.lower()
            }
    
    def process_pdf_with_table_extraction(self, pdf_path: str, target_tables: List[str] = None, original_filename: str = None) -> Dict[str, Any]:
        """Processa PDF e extrai tabelas específicas"""
        if target_tables is None:
            target_tables = [
                "investimento_financeiro",
                "renda_fixa",
                "valores_contrato", 
                "produtos_servicos",
                "cronograma_pagamentos",
                "partes_contrato"
            ]
        
        display_name = original_filename if original_filename else os.path.basename(pdf_path)
        print(f"Processando PDF: {display_name}")
        
        # Extrai texto do PDF
        pdf_data = self.extract_text_from_pdf(pdf_path)
        if not pdf_data or 'error' in pdf_data:
            error_detail = pdf_data.get('error', 'Erro desconhecido na extração de texto') if pdf_data else 'Erro ao extrair texto do PDF'
            
            # Verificar se é erro de dependência
            if pdf_data and pdf_data.get('is_dependency_error'):
                print(f"Erro com PyCryptodome")
                return {
                    'error': error_detail,
                    'solution': 'Execute: pip install PyCryptodome',
                    'error_type': 'dependency'
                }
            
            # Verificar se é erro de criptografia
            if pdf_data and (pdf_data.get('is_encrypted') or pdf_data.get('is_encrypted_error')):
                print(f"PDF criptografado detectado")
                return {
                    'error': error_detail,
                    'solution': 'PDF protegido por senha. Remova a proteção ou forneça a senha.',
                    'error_type': 'encryption'
                }
            
            return {'error': error_detail}
        
        # Extrai tabelas usando IA
        tables = {}
        if self.model:
            tables = self.extract_tables_with_ai(pdf_data, target_tables)
        
        # Salva tabelas como CSV no S3
        s3_csv_files = {}
        if tables:
            # Use o nome original se fornecido, senão use o caminho do arquivo
            if original_filename:
                base_filename = os.path.splitext(original_filename)[0]
            else:
                base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
            s3_csv_files = self.save_tables_to_s3_csv(tables, base_filename)
        
        # Gera e salva tabelas Delta no S3
        s3_delta_files = {}
        if tables:
            # Use o nome original se fornecido, senão use o caminho do arquivo
            filename_for_delta = original_filename if original_filename else os.path.basename(pdf_path)
            s3_delta_files = self.generate_and_save_delta_tables_to_s3(tables, filename_for_delta)
            # s3_delta_files = self.generate_and_save_delta_tables_to_s3(tables, os.path.basename(pdf_path))
        
        # Converte DataFrames para formato JSON-friendly
        json_friendly_tables = self.convert_dataframes_to_json_friendly(tables)
        
        return {
            'pdf_info': {
                'page_count': pdf_data['page_count'],
                'total_chars': pdf_data['total_chars'],
                'title': original_filename if original_filename else os.path.basename(pdf_path)
            },
            'tables_extracted': list(tables.keys()),
            'tables': json_friendly_tables,
            's3_csv_files': s3_csv_files,
            's3_delta_files': s3_delta_files,
            'processing_date': datetime.now().isoformat()
        }
    
    def extract_tables_with_ai(self, pdf_data: Dict[str, Any], target_tables: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Extrai tabelas estruturadas de dados PDF usando modelos de IA (Bedrock ou Google).
        Este método utiliza modelos de linguagem para identificar e extrair tabelas específicas
        do texto de um PDF, convertendo-as em DataFrames pandas estruturados. Suporta tanto
        AWS Bedrock (Claude) quanto Google Gemini como provedores de IA.
        Args:
            pdf_data (Dict[str, Any]): Dicionário contendo os dados extraídos do PDF,
                deve incluir a chave 'full_text' com o texto completo do documento.
            target_tables (List[str]): Lista de tipos/nomes das tabelas a serem extraídas
                do documento PDF.
        Returns:
            Dict[str, pd.DataFrame]: Dicionário onde as chaves são os tipos de tabela
                e os valores são DataFrames pandas com os dados extraídos. Retorna
                dicionário vazio se nenhum modelo estiver disponível.
        Raises:
            Exception: Captura e registra erros durante a extração de tabelas individuais,
                mas continua o processamento das demais tabelas.
        Note:
            - Requer que self.model esteja configurado (Bedrock ou Google)
            - Utiliza self.create_extraction_prompt() para gerar prompts específicos
            - Utiliza self.parse_ai_response() para processar as respostas da IA
            - Métricas de desempenho são armazenadas em self.processing_metrics
        """
        """Usa IA para extrair tabelas estruturadas"""
        if not self.model:
            print("IA não disponível para extração estruturada")
            return {}
        
        results = {}
        full_text = pdf_data['full_text']
        
        ##### Armazena métricas de processamento
        self.processing_metrics = {
            'model_used': None,
            'total_processing_time': 0,
            'tables_processed': []
        }
        
        #### Define modelo usado
        if self.model_type == "bedrock":
            self.processing_metrics['model_used'] = "anthropic.claude-3-5-sonnet-20240620-v1:0"
        elif self.model_type == "google":
            self.processing_metrics['model_used'] = "google-gemini-2.5-flash-lite-preview-06-17"
        
        start_total_time = time.time()
        
        for table_type in target_tables:
            print(f"Extraindo tabela: {table_type}")
            
            try:
                start_time = time.time()
                prompt = self.create_extraction_prompt(table_type, full_text)
                
                # Usa o modelo apropriado
                if self.model_type == "bedrock":
                    response = self.model.invoke(prompt)
                    response_text = response.content
                elif self.model_type == "google":
                    response = self.model.generate_content(prompt)
                    response_text = response.text
                else:
                    print(f"Nenhum modelo disponível para {table_type}")
                    continue
                
                processing_time = time.time() - start_time
                
                table_data = self.parse_ai_response(response_text, table_type)
                
                if table_data:
                    df = pd.DataFrame(table_data)
                    results[table_type] = df
                    print(f"Tabela {table_type}: {len(df)} linhas extraídas em {processing_time:.3f}s")
                    
                    # Armazena métricas da tabela
                    self.processing_metrics['tables_processed'].append({
                        'table_type': table_type,
                        'processing_time': round(processing_time, 3),
                        'rows_extracted': len(df)
                    })
                    
            except Exception as e:
                print(f"Erro ao extrair {table_type}: {str(e)}")
                continue
        
        self.processing_metrics['total_processing_time'] = round(time.time() - start_total_time, 3)
        return results
    
    def convert_dataframes_to_json_friendly(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Converte DataFrames pandas para formato JSON-friendly"""
        json_friendly_tables = {}
        
        for table_name, df in tables.items():
            if not df.empty:
                # Converte DataFrame para dicionário com conversão de tipos
                df_copy = df.copy()
                
                # Converte tipos numpy para tipos Python nativos
                for col in df_copy.columns:
                    if df_copy[col].dtype == 'bool':
                        df_copy[col] = df_copy[col].astype(bool)
                    elif 'int' in str(df_copy[col].dtype):
                        df_copy[col] = df_copy[col].astype(int)
                    elif 'float' in str(df_copy[col].dtype):
                        df_copy[col] = df_copy[col].astype(float)
                    else:
                        df_copy[col] = df_copy[col].astype(str)
                
                # Converte para lista de dicionários
                json_friendly_tables[table_name] = {
                    'data': df_copy.to_dict('records'),
                    'columns': list(df_copy.columns),
                    'shape': df_copy.shape,
                    'total_rows': len(df_copy)
                }
            else:
                json_friendly_tables[table_name] = {
                    'data': [],
                    'columns': [],
                    'shape': [0, 0],
                    'total_rows': 0
                }
        
        return json_friendly_tables
    
    def create_extraction_prompt(self, table_type: str, text: str) -> str:
        """Cria prompt específico para cada tipo de tabela"""
        base_prompt = f"""
            Analise o seguinte texto de documento e extraia dados para criar uma tabela do tipo: {table_type}

            TEXTO DO DOCUMENTO:
            {text[:3000]}...
        """

        if table_type == "investimento_financeiro":
            return base_prompt + """
                Extraia os principais indicadores financeiros descritos no texto.

                Retorne um JSON com um array de objetos no seguinte formato:
                [
                    {
                        "empresa": "Nome da empresa",
                        "periodo": "Período de referência (ex: 1T25)",
                        "receita_bruta": valor da receita bruta (float),
                        "variacao_receita_bruta": variação percentual da receita bruta em relação ao período anterior (float),
                        "ebitda": valor do EBITDA ou EBITDA Ajustado (float),
                        "margem_ebitda": margem EBITDA ou EBITDA Ajustado (float, em percentual),
                        "variacao_ebitda": variação percentual do EBITDA ou EBITDA Ajustado em relação ao período anterior (float),
                        "lucro_liquido": valor do lucro líquido ou lucro líquido ajustado (float),
                        "margem_lucro_liquido": margem do lucro líquido ou lucro líquido ajustado (float, em percentual),
                        "variacao_lucro_liquido": variação percentual do lucro líquido ou lucro líquido ajustado em relação ao período anterior (float)
                    },
                    ...
                ]

                Instruções:
                - Priorize os valores ajustados (EBITDA Ajustado e Lucro Líquido Ajustado), quando disponíveis.
                - Extraia também as margens correspondentes, em percentual (ex: "21,9%" → 21.9).
                - Extraia a variação percentual entre o período atual e o mesmo período do ano anterior (ex: "crescimento de 33%" → 33.0).
                - Os valores monetários devem estar em float, mesmo que apresentados em milhares ou bilhões de reais.
                - Caso alguma informação não esteja disponível, preencha com null ou string vazia.

                Responda APENAS com o JSON válido:
            """

        elif table_type == "renda_fixa":
            return base_prompt + """
                Extraia informações sobre os investimentos em renda fixa descritos no texto.

                Retorne um JSON com um array de objetos no seguinte formato:
                [
                    {
                        "banco": nome da instituição financeira (string),
                        "tipo_carteira": tipo da carteira, ex: RENDA FIXA (string),
                        "tipo": tipo do investimento, ex: PÓS-FIXADA ou INFLAÇÃO (string),
                        "ativo": nome do ativo (string),
                        "data_aplicacao": data da aplicação no formato DD/MM/AAAA (string),
                        "data_carencia": data da carência no formato DD/MM/AAAA (string),
                        "data_vencimento": data de vencimento no formato DD/MM/AAAA (string),
                        "taxa_compra": taxa de compra, ex: 121,00%CDI (string),
                        "disponivel": se está disponível para movimentação (boolean),
                        "garantia": se possui garantia (boolean),
                        "valor_aplicado": valor bruto aplicado (float),
                        "posicao_taxa_compra": posição na taxa de compra (float),
                        "valor_liquido": valor líquido atual (float)
                    },
                    ...
                ]

                Tipos de campo:
                - "banco": nome da instituição (string)
                - "tipo_carteira": tipo da carteira (string)
                - "tipo": tipo do investimento (string)
                - "ativo": nome do ativo (string)
                - "data_aplicacao", "data_carencia", "data_vencimento": datas em formato DD/MM/AAAA (string)
                - "taxa_compra": texto da taxa de compra (string)
                - "disponivel", "garantia": valores booleanos (true ou false)
                - "valor_aplicado", "posicao_taxa_compra", "valor_liquido": valores monetários (float)

                Responda APENAS com o JSON válido:
            """
        
        elif table_type == "valores_contrato":
            return base_prompt + """
                    Extraia TODOS os valores monetários encontrados e suas descrições.
                    Retorne um JSON com array de objetos no formato:
                    [
                        {"Descricao": "valor total do contrato", "valor": 1234.56},
                        {"Descricao": "parcela 1 da entrada", "valor": 2345.67}
                    ]

                    Inclua valores como:
                    - Valor total do contrato
                    - Parcelas e entrada
                    - Taxas e impostos
                    - Multas e penalidades
                    - Descontos

                    Responda APENAS com o JSON válido:
            """
        
        elif table_type == "produtos_servicos":
            return base_prompt + """
                Extraia TODOS os produtos, serviços ou itens mencionados.
                Retorne um JSON com array de objetos no formato:
                [
                    {"produto_servico": "Nome do item", "detalhes": "Descrição/especificações", "categoria": "tipo"},
                    {"produto_servico": "Outro item", "detalhes": "Outras especificações", "categoria": "tipo"}
                ]

                Inclua itens como:
                - Produtos físicos
                - Serviços prestados
                - Equipamentos
                - Materiais
                - Qualquer item comercializado

                Responda APENAS com o JSON válido:
            """
        
        elif table_type == "cronograma_pagamentos":
            return base_prompt + """
                Extraia informações sobre cronograma de pagamentos e datas.
                Retorne um JSON com array de objetos no formato:
                [
                    {"Data/Prazo": "data ou prazo", "Descrição": "o que deve ser pago/feito", "Valor": "valor se houver"},
                    {"Data/Prazo": "outra data", "Descrição": "outra ação", "Valor": "outro valor"}
                ]

                Inclua:
                - Datas de pagamento
                - Prazos de entrega
                - Vencimentos
                - Cronogramas
                - Marcos do projeto

                Responda APENAS com o JSON válido:
            """
        
        elif table_type == "partes_contrato":
            return base_prompt + """
                Extraia informações sobre as partes envolvidas no contrato.
                Retorne um JSON com array de objetos no formato:
                [
                    {"Tipo": "Contratante/Contratado/etc", "Nome": "nome da pessoa/empresa", "Documento": "CPF/CNPJ", "Endereço": "endereço se disponível"},
                    {"Tipo": "outro tipo", "Nome": "outro nome", "Documento": "outro doc", "Endereço": "outro endereço"}
                ]

                Inclua:
                - Contratante
                - Contratado
                - Testemunhas
                - Avalistas
                - Qualquer parte mencionada

                Responda APENAS com o JSON válido:
            """
        
        else:
            return base_prompt + f"""
                Extraia dados relevantes para criar uma tabela sobre: {table_type}
                Analise o contexto e retorne um JSON com array de objetos adequado ao tipo de dados.
                Seja criativo e específico baseado no conteúdo do documento.

                Responda APENAS com o JSON válido:
            """
    
    def parse_ai_response(self, response_text: str, table_type: str) -> List[Dict[str, Any]]:
        """Faz parse da resposta da IA com tratamento de erros"""
        try:
            print(f"Processando resposta da IA para {table_type}...")
            
            # Remove markdown se presente
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0]
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0]
            
            # Limpa a resposta
            response_text = response_text.strip()
            
            # Tenta múltiplas estratégias para encontrar o JSON
            json_candidates = []
            
            # Estratégia 1: Procura por [ ... ]
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_candidates.append(response_text[json_start:json_end])
            
            # Estratégia 2: Procura por múltiplos blocos JSON
            json_blocks = re.findall(r'\[.*?\]', response_text, re.DOTALL)
            json_candidates.extend(json_blocks)
            
            # Tenta fazer parse de cada candidato
            for i, json_text in enumerate(json_candidates):
                try:
                    data = json.loads(json_text)
                    
                    if isinstance(data, list) and len(data) > 0:
                        print(f"JSON válido encontrado na tentativa {i+1} para {table_type}")
                        return data
                    
                except json.JSONDecodeError as e:
                    print(f"Tentativa {i+1} falhou para {table_type}: {str(e)}")
                    continue
            
            # Se chegou aqui, não conseguiu fazer parse
            print(f"Nenhum JSON válido encontrado para {table_type}")
            # print(f"Resposta original (primeiros 500 chars): {response_text[:500]}")
            
            return []
            
        except Exception as e:
            print(f"Erro inesperado ao processar resposta para {table_type}: {str(e)}")
            return []
    
    def save_tables_to_s3_csv(self, tables: Dict[str, pd.DataFrame], base_filename: str) -> Dict[str, str]:
        """Salva tabelas como CSV no S3"""
        if not self.s3_client:
            print("S3 não configurado. Salvando localmente...")
            return self.save_tables_locally(tables, base_filename)
        
        saved_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for table_type, df in tables.items():
            if not df.empty:
                # Prepara nome do arquivo
                filename = f"{base_filename}_{table_type}_{timestamp}.csv"
                s3_key = f"{self.s3_folder}/csv/{filename}"
                
                try:
                    # Converte DataFrame para CSV em memória
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False, encoding='utf-8')
                    csv_content = csv_buffer.getvalue()
                    
                    # Upload para S3
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=csv_content,
                        ContentType='text/csv'
                    )
                    
                    s3_path = f"s3://{self.bucket_name}/{s3_key}"
                    saved_files[table_type] = s3_path
                    
                    print(f"Tabela {table_type} salva: {s3_path} ({len(df)} linhas)")
                    
                except Exception as e:
                    print(f"Erro ao salvar {table_type} no S3: {str(e)}")
                    continue
        
        return saved_files
    
    def generate_and_save_delta_tables_to_s3(self, tables_data: Dict[str, pd.DataFrame], filename: str) -> Dict[str, str]:
        """
        Gera e salva tabelas delta no S3 com base nos dados extraídos de PDFs financeiros.
        Esta função processa dados de investimento financeiro e renda fixa, criando tabelas delta
        especializadas que são salvas no Amazon S3. As tabelas principais (investimento e renda fixa)
        são salvas com nomes fixos e suportam append de dados, enquanto tabelas analíticas recebem
        timestamps únicos.
        Args:
            tables_data (Dict[str, pd.DataFrame]): Dicionário contendo DataFrames com dados extraídos
                do PDF, incluindo possíveis chaves como 'investimento_financeiro', 'renda_fixa',
                'produtos_servicos', 'valores_contrato', 'cronograma_pagamentos'.
            filename (str): Nome do arquivo PDF original, usado para nomenclatura das tabelas delta
                e identificação nos logs.
        Returns:
            Dict[str, str]: Dicionário com informações das tabelas delta criadas, onde as chaves
                representam o tipo de delta ('delta_investimento', 'delta_renda_fixa', 
                'delta_produto_valor', 'delta_fluxo_caixa') e os valores contêm informações
                sobre localização no S3 ou status da operação.
        Note:
            - A função verifica a presença de dados antes de tentar gerar cada tipo de tabela delta
            - Tabelas principais (investimento/renda fixa) usam nomes fixos e suportam append
            - Tabelas analíticas recebem timestamps para evitar conflitos
            - Se não houver dados de investimento nem renda fixa, nenhuma tabela é gerada
            - Logs detalhados são gerados para acompanhar o processo de criação das tabelas
        """
        """Gera tabelas deltas e salva no S3 com estrutura Delta completa"""
        print(f"Gerando tabelas deltas para {filename}...")
        
        delta_files = {}
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        base_filename = os.path.splitext(filename)[0]
        
        # Verifica se existe estrutura adequada para gerar tabelas Delta principais
        has_investment_data = False
        has_renda_fixa_data = False
        
        # Verifica se há dados de investimento financeiro
        if 'investimento_financeiro' in tables_data and not tables_data['investimento_financeiro'].empty:
            has_investment_data = True
            print(f"Dados de investimento financeiro encontrados: {len(tables_data['investimento_financeiro'])} linhas")
        else:
            print("Nenhum dado de investimento financeiro encontrado no PDF")
        
        # Verifica se há dados de renda fixa
        if 'renda_fixa' in tables_data and not tables_data['renda_fixa'].empty:
            has_renda_fixa_data = True
            print(f"Dados de renda fixa encontrados: {len(tables_data['renda_fixa'])} linhas")
        else:
            print("Nenhum dado de renda fixa encontrado no PDF")
        
        # Se não há dados de investimento nem renda fixa, não gera nenhuma tabela Delta
        if not has_investment_data and not has_renda_fixa_data:
            print("Nenhuma estrutura adequada encontrada para gerar tabelas Delta")
            print("PDF não contém dados de investimento financeiro nem renda fixa - pulando geração de tabelas Delta")
            return delta_files
        
        try:
            # Delta 1: Tabela de Investimentos (nome fixo com append) - só processa se há dados
            if has_investment_data:
                financial_delta = self.create_financial_delta_table(tables_data, filename)
                if not financial_delta.empty:
                    delta_info = self.save_or_append_delta_table_to_s3(financial_delta, "investimento", filename)
                    delta_files['delta_investimento'] = delta_info
                    print(f"Delta Investimento: {len(financial_delta)} linhas adicionadas -> {delta_info}")
            
            # Delta 1.1: Tabela de Renda Fixa (nome fixo com append) - só processa se há dados
            if has_renda_fixa_data:
                renda_fixa_delta = self.create_renda_fixa_delta_table(tables_data, filename)
                if not renda_fixa_delta.empty:
                    delta_info = self.save_or_append_delta_table_to_s3(renda_fixa_delta, "renda_fixa", filename)
                    delta_files['delta_renda_fixa'] = delta_info
                    print(f"Delta Renda Fixa: {len(renda_fixa_delta)} linhas adicionadas -> {delta_info}")
            
            # Delta 2: Análise de Produtos/Serviços vs Valores - só processa se há dados de investimento
            if has_investment_data and ('produtos_servicos' in tables_data or 'investimento_financeiro' in tables_data) and \
               ('valores_contrato' in tables_data or any('valor' in k for k in tables_data.keys())):
                product_value_delta = self.create_product_value_delta_table(tables_data, filename)
                if not product_value_delta.empty:
                    delta_filename = f"{base_filename}_delta_produto_valor_{timestamp}"
                    delta_info = self.save_delta_table_to_s3(product_value_delta, delta_filename)
                    delta_files['delta_produto_valor'] = delta_info
                    print(f"Delta Produto-Valor: {len(product_value_delta)} linhas -> {delta_info}")
            
            # Delta 3: Cronograma vs Valores (Fluxo de Caixa) - só processa se há dados de investimento
            if has_investment_data and 'cronograma_pagamentos' in tables_data and \
               ('valores_contrato' in tables_data or any('valor' in k for k in tables_data.keys())):
                cashflow_delta = self.create_cashflow_delta_table(tables_data, filename)
                if not cashflow_delta.empty:
                    delta_filename = f"{base_filename}_delta_fluxo_caixa_{timestamp}"
                    delta_info = self.save_delta_table_to_s3(cashflow_delta, delta_filename)
                    delta_files['delta_fluxo_caixa'] = delta_info
                    print(f"Delta Fluxo de Caixa: {len(cashflow_delta)} linhas -> {delta_info}")
            
            if len(delta_files) > 0:
                print(f"{len(delta_files)} tabelas deltas geradas")
            else:
                print("Nenhuma tabela Delta foi gerada - dados insuficientes")
            
        except Exception as e:
            print(f"Erro ao gerar tabelas deltas: {str(e)}")
            
        return delta_files
    
    def create_delta_metadata(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Cria metadados Delta para a tabela"""
        
        # Gera IDs únicos
        content_hash = hashlib.md5(df.to_csv(index=False).encode()).hexdigest()
        file_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{content_hash[:8]}"
        table_id = str(uuid.uuid4())
        txn_id = str(uuid.uuid4())
        
        # Schema em formato Delta
        schema_fields = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            delta_type = self.pandas_to_delta_type(dtype)
            schema_fields.append({
                "name": col,
                "type": delta_type,
                "nullable": True,
                "metadata": {}
            })
        
        timestamp = int(datetime.now().timestamp() * 1000)
        
        # Calcula estatísticas para o arquivo add
        stats = {
            "numRecords": len(df),
            "minValues": {},
            "maxValues": {},
            "nullCount": {}
        }
        
        if len(df) > 0:
            for col in df.columns:
                try:
                    if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                        stats["minValues"][col] = float(df[col].min()) if pd.notna(df[col].min()) else None
                        stats["maxValues"][col] = float(df[col].max()) if pd.notna(df[col].max()) else None
                    else:
                        stats["minValues"][col] = str(df[col].min()) if pd.notna(df[col].min()) else None
                        stats["maxValues"][col] = str(df[col].max()) if pd.notna(df[col].max()) else None
                    stats["nullCount"][col] = int(df[col].isnull().sum())
                except:
                    # Se houver erro, ignora a coluna
                    pass
        
        delta_metadata = {
            "commitInfo": {
                "timestamp": timestamp,
                "operation": "WRITE",
                "operationParameters": {
                    "mode": "Append",
                    "partitionBy": "[]"
                },
                "isolationLevel": "Serializable",
                "isBlindAppend": False,
                "operationMetrics": {
                    "numFiles": "1",
                    "numOutputRows": str(len(df)),
                    "numOutputBytes": "0"  # Será atualizado após salvar
                },
                "engineInfo": "Apache-Spark/3.5.3 Delta-Lake/3.2.1",
                "txnId": txn_id
            },
            "metaData": {
                "id": table_id,
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": json.dumps({
                    "type": "struct",
                    "fields": schema_fields
                }, separators=(',', ':')),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": timestamp
            },
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2
            },
            "add": {
                "path": f"part-00000-{content_hash[:8]}-{file_id[-8:]}-c000.snappy.parquet",
                "partitionValues": {},
                "size": 0,  # Será atualizado após salvar
                "modificationTime": timestamp,
                "dataChange": True,
                "stats": json.dumps(stats, separators=(',', ':'))
            },
            "file_id": file_id,
            "table_id": table_id,
            "txn_id": txn_id
        }
        
        return delta_metadata
    
    def create_delta_append_metadata(self, df: pd.DataFrame, table_name: str, added_rows: int) -> Dict[str, Any]:
        """Cria metadados Delta para operação de append"""
        
        # Gera IDs únicos
        content_hash = hashlib.md5(df.to_csv(index=False).encode()).hexdigest()
        file_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{content_hash[:8]}"
        txn_id = str(uuid.uuid4())
        
        timestamp = int(datetime.now().timestamp() * 1000)
        
        # Calcula estatísticas para o arquivo add
        stats = {
            "numRecords": added_rows,
            "minValues": {},
            "maxValues": {},
            "nullCount": {}
        }
        
        if len(df) > 0:
            for col in df.columns:
                try:
                    if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                        stats["minValues"][col] = float(df[col].min()) if pd.notna(df[col].min()) else None
                        stats["maxValues"][col] = float(df[col].max()) if pd.notna(df[col].max()) else None
                    else:
                        stats["minValues"][col] = str(df[col].min()) if pd.notna(df[col].min()) else None
                        stats["maxValues"][col] = str(df[col].max()) if pd.notna(df[col].max()) else None
                    stats["nullCount"][col] = int(df[col].isnull().sum())
                except:
                    # Se houver erro, ignora a coluna
                    pass
        
        delta_metadata = {
            "commitInfo": {
                "timestamp": timestamp,
                "operation": "WRITE",
                "operationParameters": {
                    "mode": "Append",
                    "partitionBy": "[]"
                },
                "isolationLevel": "Serializable",
                "isBlindAppend": False,
                "operationMetrics": {
                    "numFiles": "1",
                    "numOutputRows": str(added_rows),
                    "numOutputBytes": "0"  # Será atualizado após salvar
                },
                "engineInfo": "Apache-Spark/3.5.3 Delta-Lake/3.2.1",
                "txnId": txn_id
            },
            "add": {
                "path": f"part-00001-{content_hash[:8]}-{file_id[-8:]}-c000.snappy.parquet",
                "partitionValues": {},
                "size": 0,  # Será atualizado após salvar
                "modificationTime": timestamp,
                "dataChange": True,
                "stats": json.dumps(stats, separators=(',', ':'))
            },
            "file_id": file_id,
            "txn_id": txn_id
        }
        
        return delta_metadata
    
    def pandas_to_delta_type(self, pandas_dtype: str) -> str:
        """Converte tipos pandas para tipos Delta Lake"""
        dtype_mapping = {
            'int64': 'long',
            'int32': 'integer',
            'float64': 'double',
            'float32': 'float',
            'object': 'string',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp',
            'category': 'string'
        }
        
        return dtype_mapping.get(pandas_dtype, 'string')
    
    def create_financial_delta_table(self, tables_data: Dict[str, pd.DataFrame], filename: str) -> pd.DataFrame:
        """Converte tabela investimento_financeiro para Delta"""
        
        # Procura pela tabela investimento_financeiro
        for table_name, df in tables_data.items():
            if 'investimento_financeiro' in table_name.lower():
                if not df.empty:
                    print(f"Convertendo tabela {table_name} para Delta")
                    # Adiciona colunas de rastreamento
                    df_copy = df.copy()
                    df_copy['arquivo_origem'] = filename
                    df_copy['data_processamento'] = datetime.now().isoformat()
                    df_copy['hash_linha'] = df_copy.apply(lambda x: hash(str(x.values)), axis=1)
                    return df_copy
        
        # Se não encontrou investimento_financeiro, retorna DataFrame vazio
        print("Tabela investimento_financeiro não encontrada")
        return pd.DataFrame()
    
    def create_renda_fixa_delta_table(self, tables_data: Dict[str, pd.DataFrame], filename: str) -> pd.DataFrame:
        """Converte tabela renda_fixa para Delta"""
        
        # Procura pela tabela renda_fixa
        for table_name, df in tables_data.items():
            if 'renda_fixa' in table_name.lower():
                if not df.empty:
                    print(f"Convertendo tabela {table_name} para Delta")
                    # Adiciona colunas de rastreamento
                    df_copy = df.copy()
                    df_copy['arquivo_origem'] = filename
                    df_copy['data_processamento'] = datetime.now().isoformat()
                    df_copy['hash_linha'] = df_copy.apply(lambda x: hash(str(x.values)), axis=1)
                    return df_copy
        
        # Se não encontrou renda_fixa, retorna DataFrame vazio
        print("Tabela renda_fixa não encontrada")
        return pd.DataFrame()
    
    def create_product_value_delta_table(self, tables_data: Dict[str, pd.DataFrame], filename: str) -> pd.DataFrame:
        """Cria tabela delta correlacionando produtos/serviços com valores"""
        correlations = []
        
        # Extrai o nome do arquivo PDF original sem extensão
        pdf_filename = os.path.splitext(os.path.basename(filename))[0] if filename else "unknown"
        
        # Obtém métricas de processamento se disponíveis
        model_used = getattr(self, 'processing_metrics', {}).get('model_used', 'modelo_nao_identificado')
        total_processing_time = getattr(self, 'processing_metrics', {}).get('total_processing_time', 0)
        
        products_df = tables_data.get('produtos_servicos') or tables_data.get('investimento_financeiro')
        values_df = tables_data.get('valores_contrato')
        
        if products_df is not None and not products_df.empty:
            for i, product_row in products_df.iterrows():
                product_name = str(list(product_row.values)[0])  # Primeiro valor como nome do produto
                
                # Busca valores relacionados
                estimated_value = 0
                value_confidence = 'Baixa'
                
                if values_df is not None and not values_df.empty:
                    for _, value_row in values_df.iterrows():
                        value_desc = str(value_row.get('valor', value_row.get('Valor', 0)))
                        if any(word in value_desc.lower() for word in product_name.lower().split()):
                            try:
                                value_str = str(value_row.get('valor', value_row.get('Valor', 0)))
                                estimated_value = float(re.findall(r'\d+\.?\d*', value_str.replace(',', '.'))[0])
                                value_confidence = 'Alta'
                                break
                            except:
                                continue
                
                correlations.append({
                    'Arquivo': filename,
                    'Produto_Servico': product_name,
                    'Valor_Estimado': estimated_value,
                    'Confianca_Valor': value_confidence,
                    'Data_Analise': datetime.now().strftime('%d/%m/%Y %H:%M'),
                    'data_processamento': datetime.now().isoformat(),
                    'modelo_llm_usado': model_used,
                    'tempo_processamento_llm_segundos': total_processing_time
                })
        
        return pd.DataFrame(correlations)
    
    def create_cashflow_delta_table(self, tables_data: Dict[str, pd.DataFrame], filename: str) -> pd.DataFrame:
        """Cria tabela delta de fluxo de caixa baseada em cronograma e valores"""
        cashflow_data = []
        
        schedule_df = tables_data.get('cronograma_pagamentos')
        values_df = tables_data.get('valores_contrato')
        
        if schedule_df is not None and not schedule_df.empty and values_df is not None and not values_df.empty:
            total_contract_value = 0
            
            # Calcula valor total do contrato
            for _, value_row in values_df.iterrows():
                try:
                    value_str = str(value_row.get('valor', value_row.get('Valor', 0)))
                    value = float(re.findall(r'\d+\.?\d*', value_str.replace(',', '.'))[0])
                    total_contract_value += value
                except:
                    continue
            
            # Distribui valores pelo cronograma
            for i, schedule_row in schedule_df.iterrows():
                date_field = schedule_row.get('Data/Prazo', schedule_row.get('Data', ''))
                description = schedule_row.get('Descrição', schedule_row.get('Descricao', ''))
                schedule_value = schedule_row.get('Valor', 0)
                
                # Se não tem valor específico, estima baseado na descrição
                if not schedule_value or schedule_value == 0:
                    if 'entrada' in description.lower():
                        estimated_value = total_contract_value * 0.3
                    elif 'parcela' in description.lower():
                        estimated_value = total_contract_value * 0.2
                    elif 'final' in description.lower():
                        estimated_value = total_contract_value * 0.1
                    else:
                        estimated_value = total_contract_value / len(schedule_df)
                else:
                    estimated_value = schedule_value
                
                cashflow_data.append({
                    'Arquivo': filename,
                    'Data_Prevista': date_field,
                    'Descricao_Fluxo': description,
                    'Valor_Estimado': estimated_value,
                    'Percentual_Total': (estimated_value / total_contract_value * 100) if total_contract_value > 0 else 0,
                    'Data_Analise': datetime.now().strftime('%d/%m/%Y %H:%M'),
                    'data_processamento': datetime.now().isoformat()
                })
        
        return pd.DataFrame(cashflow_data)
    
    def save_or_append_delta_table_to_s3(self, df: pd.DataFrame, table_name: str, filename: str) -> str:
        """Salva ou faz append de tabela Delta no S3 com estrutura completa"""
        if not self.s3_client:
            return f"S3 não configurado - dados salvos localmente"
        
        try:
            # Cria diretório temporário para gerar estrutura Delta
            with tempfile.TemporaryDirectory() as temp_dir:
                delta_path = os.path.join(temp_dir, table_name)
                os.makedirs(delta_path, exist_ok=True)
                
                # Verifica se a tabela já existe no S3
                s3_delta_path = f"{self.s3_folder}/delta_datasets/{table_name}/"
                
                existing_data = self.check_existing_delta_table_in_s3(s3_delta_path)
                
                if existing_data:
                    # Tabela existe - faz append
                    print(f"Tabela Delta '{table_name}' já existe no S3. Fazendo append de {len(df)} novas linhas...")
                    
                    # Adiciona colunas de rastreamento
                    df_copy = df.copy()
                    df_copy['fonte_arquivo'] = filename
                    df_copy['data_insercao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Extrai o nome do arquivo PDF original sem extensão
                    pdf_filename = os.path.splitext(os.path.basename(filename))[0] if filename else "unknown"
                    
                    # Obtém métricas de processamento se disponíveis
                    model_used = getattr(self, 'processing_metrics', {}).get('model_used', 'modelo_nao_identificado')
                    total_processing_time = getattr(self, 'processing_metrics', {}).get('total_processing_time', 0)
                    
                    # Adiciona informações de processamento
                    df_copy['Arquivo'] = filename
                    df_copy['modelo_llm_usado'] = model_used
                    df_copy['tempo_processamento_llm_segundos'] = total_processing_time
                    
                    # Combina com dados existentes (simplificado - em produção seria mais complexo)
                    combined_df = df_copy  # Para simplificar, não fazemos download dos dados existentes
                    
                    # Gera metadados de append
                    delta_metadata = self.create_delta_append_metadata(combined_df, table_name, len(df))
                    
                    # Salva arquivo Parquet
                    parquet_filename = delta_metadata['add']['path']
                    parquet_file = os.path.join(delta_path, parquet_filename)
                    combined_df.to_parquet(parquet_file, engine='pyarrow', index=False)
                    
                    # Cria estrutura de log
                    os.makedirs(os.path.join(delta_path, "_delta_log"), exist_ok=True)
                    
                    # Salva metadados delta (nova versão) - formato Delta Lake correto
                    version_number = 1  # Simplificado - em produção seria calculado
                    version_file = os.path.join(delta_path, "_delta_log", f"{version_number:020d}.json")
                    
                    # Atualiza tamanho do arquivo nos metadados
                    delta_metadata['add']['size'] = os.path.getsize(parquet_file)
                    delta_metadata['commitInfo']['operationMetrics']['numOutputBytes'] = str(os.path.getsize(parquet_file))
                    
                    with open(version_file, 'w', encoding='utf-8') as f:
                        # Formato Delta Lake para append: apenas commitInfo e add
                        f.write(json.dumps({"commitInfo": delta_metadata['commitInfo']}, ensure_ascii=False, separators=(',', ':')))
                        f.write('\n')
                        f.write(json.dumps({"add": delta_metadata['add']}, ensure_ascii=False, separators=(',', ':')))
                        f.write('\n')
                    
                    # Upload da estrutura completa para S3
                    self.upload_delta_structure_to_s3(delta_path, s3_delta_path)
                    
                    s3_path = f"s3://{self.bucket_name}/{s3_delta_path}"
                    print(f"Append realizado com sucesso! Novo arquivo: {s3_path}")
                    
                    # Configura compatibilidade com Athena (atualiza manifest)
                    athena_config = self.setup_athena_compatibility(table_name, combined_df, s3_path)
                    
                    return s3_path
                    
                else:
                    # Tabela não existe - cria nova
                    print(f"Criando nova tabela Delta '{table_name}' no S3 com {len(df)} linhas...")
                    
                    # Adiciona colunas de rastreamento
                    df_copy = df.copy()
                    df_copy['fonte_arquivo'] = filename
                    df_copy['data_insercao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Extrai o nome do arquivo PDF original sem extensão
                    pdf_filename = os.path.splitext(os.path.basename(filename))[0] if filename else "unknown"
                    
                    # Obtém métricas de processamento se disponíveis
                    model_used = getattr(self, 'processing_metrics', {}).get('model_used', 'modelo_nao_identificado')
                    total_processing_time = getattr(self, 'processing_metrics', {}).get('total_processing_time', 0)
                    
                    # Adiciona informações de processamento
                    df_copy['Arquivo'] = filename
                    df_copy['modelo_llm_usado'] = model_used
                    df_copy['tempo_processamento_llm_segundos'] = total_processing_time
                    
                    # Gera metadados delta
                    delta_metadata = self.create_delta_metadata(df_copy, table_name)
                    
                    # Salva arquivo Parquet
                    parquet_filename = delta_metadata['add']['path']
                    parquet_file = os.path.join(delta_path, parquet_filename)
                    df_copy.to_parquet(parquet_file, engine='pyarrow', index=False)
                    
                    # Atualiza tamanho do arquivo nos metadados
                    delta_metadata['add']['size'] = os.path.getsize(parquet_file)
                    delta_metadata['commitInfo']['operationMetrics']['numOutputBytes'] = str(os.path.getsize(parquet_file))
                    
                    # Cria estrutura de log
                    os.makedirs(os.path.join(delta_path, "_delta_log"), exist_ok=True)
                    
                    # Salva metadados delta - formato Delta Lake correto (commitInfo, metaData, protocol, add)
                    metadata_file = os.path.join(delta_path, "_delta_log", "00000000000000000000.json")
                    with open(metadata_file, 'w', encoding='utf-8') as f:
                        # Ordem correta: commitInfo, metaData, protocol, add
                        f.write(json.dumps({"commitInfo": delta_metadata['commitInfo']}, ensure_ascii=False, separators=(',', ':')))
                        f.write('\n')
                        f.write(json.dumps({"metaData": delta_metadata['metaData']}, ensure_ascii=False, separators=(',', ':')))
                        f.write('\n')
                        f.write(json.dumps({"protocol": delta_metadata['protocol']}, ensure_ascii=False, separators=(',', ':')))
                        f.write('\n')
                        f.write(json.dumps({"add": delta_metadata['add']}, ensure_ascii=False, separators=(',', ':')))
                        f.write('\n')
                    
                    # Upload da estrutura completa para S3
                    self.upload_delta_structure_to_s3(delta_path, s3_delta_path)
                    
                    s3_path = f"s3://{self.bucket_name}/{s3_delta_path}"
                    print(f"Tabela Delta criada com sucesso: {s3_path}")
                    
                    # Configura compatibilidade com Athena
                    athena_config = self.setup_athena_compatibility(table_name, df_copy, s3_path)
                    
                    return s3_path
            
        except Exception as e:
            print(f"Erro ao salvar tabela Delta no S3: {str(e)}")
            return f"Erro: {str(e)}"
    
    def check_existing_delta_table_in_s3(self, s3_delta_path: str) -> bool:
        """Verifica se uma tabela Delta já existe no S3"""
        try:
            # Tenta listar objetos no caminho da tabela Delta
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_delta_path + "_delta_log/"
            )
            
            return 'Contents' in response and len(response['Contents']) > 0
            
        except Exception as e:
            print(f"Erro ao verificar tabela Delta existente: {str(e)}")
            return False
    
    def upload_delta_structure_to_s3(self, local_delta_path: str, s3_delta_path: str):
        """Upload completo da estrutura Delta para S3"""
        for root, dirs, files in os.walk(local_delta_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_delta_path)
                s3_key = f"{s3_delta_path}{relative_path}".replace('\\', '/')
                
                # Determina content type
                content_type = 'application/json' if file.endswith('.json') else 'application/parquet'
                
                # Upload do arquivo
                with open(local_file, 'rb') as f:
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=f.read(),
                        ContentType=content_type
                    )
                
                print(f"Uploaded: {s3_key}")
    
    def save_delta_table_to_s3(self, df: pd.DataFrame, table_name: str) -> str:
        """Salva tabela Delta simples no S3"""
        if not self.s3_client:
            return f"S3 não configurado - dados salvos localmente"
        
        try:
            # Cria diretório temporário para gerar estrutura Delta
            with tempfile.TemporaryDirectory() as temp_dir:
                delta_path = os.path.join(temp_dir, table_name)
                os.makedirs(delta_path, exist_ok=True)
                
                # Gera metadados delta
                delta_metadata = self.create_delta_metadata(df, table_name)
                
                # Salva arquivo Parquet
                parquet_filename = delta_metadata['add']['path']
                parquet_file = os.path.join(delta_path, parquet_filename)
                df.to_parquet(parquet_file, engine='pyarrow', index=False)
                
                # Atualiza tamanho do arquivo nos metadados
                delta_metadata['add']['size'] = os.path.getsize(parquet_file)
                delta_metadata['commitInfo']['operationMetrics']['numOutputBytes'] = str(os.path.getsize(parquet_file))
                
                # Cria estrutura de log
                os.makedirs(os.path.join(delta_path, "_delta_log"), exist_ok=True)
                
                # Salva metadados delta - formato Delta Lake correto (commitInfo, metaData, protocol, add)
                metadata_file = os.path.join(delta_path, "_delta_log", "00000000000000000000.json")
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    # Ordem correta: commitInfo, metaData, protocol, add
                    f.write(json.dumps({"commitInfo": delta_metadata['commitInfo']}, ensure_ascii=False, separators=(',', ':')))
                    f.write('\n')
                    f.write(json.dumps({"metaData": delta_metadata['metaData']}, ensure_ascii=False, separators=(',', ':')))
                    f.write('\n')
                    f.write(json.dumps({"protocol": delta_metadata['protocol']}, ensure_ascii=False, separators=(',', ':')))
                    f.write('\n')
                    f.write(json.dumps({"add": delta_metadata['add']}, ensure_ascii=False, separators=(',', ':')))
                    f.write('\n')
                
                # Upload da estrutura completa para S3
                s3_delta_path = f"{self.s3_folder}/delta_datasets/{table_name}/"
                self.upload_delta_structure_to_s3(delta_path, s3_delta_path)
                
                s3_path = f"s3://{self.bucket_name}/{s3_delta_path}"
                print(f"Tabela Delta salva: {s3_path}")
                
                # Configura compatibilidade com Athena
                athena_config = self.setup_athena_compatibility(table_name, df, s3_path)
                
                return s3_path
            
        except Exception as e:
            print(f"Erro ao salvar tabela Delta no S3: {str(e)}")
            return f"Erro: {str(e)}"
    
    def save_tables_locally(self, tables: Dict[str, pd.DataFrame], base_filename: str) -> Dict[str, str]:
        """Fallback para salvar tabelas localmente caso S3 não esteja disponível"""
        os.makedirs("datasets", exist_ok=True)
        
        saved_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for table_type, df in tables.items():
            if not df.empty:
                filename = f"{base_filename}_{table_type}_{timestamp}.csv"
                filepath = os.path.join("datasets", filename)
                
                df.to_csv(filepath, index=False, encoding='utf-8')
                saved_files[table_type] = filepath
                
                print(f"Tabela {table_type} salva: {filepath} ({len(df)} linhas)")
        
        return saved_files
    
    def test_s3_connection(self) -> bool:
        """Testa conexão com S3"""
        if not self.s3_client:
            return False
        
        try:
            # Tenta listar objetos do bucket
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Conexão com S3 bucket '{self.bucket_name}' bem-sucedida")
            return True
        except Exception as e:
            print(f"Erro ao conectar com S3: {str(e)}")
            return False
    
    def generate_symlink_format_manifest(self, delta_table_path: str, table_name: str) -> str:
        """Gera o arquivo _symlink_format_manifest para compatibilidade com Athena"""
        try:
            print(f"Gerando symlink manifest para tabela: {table_name}")
            
            # Lista todos os arquivos Parquet na tabela Delta
            parquet_files = []
            
            # Busca arquivos Parquet no S3
            s3_prefix = f"{self.s3_folder}/delta_datasets/{table_name}/"
            print(f"Buscando arquivos Parquet em: {s3_prefix}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix
            )
            
            if 'Contents' in response:
                print(f"Encontrados {len(response['Contents'])} objetos no S3")
                for obj in response['Contents']:
                    print(f"Objeto encontrado: {obj['Key']}")
                    if obj['Key'].endswith('.parquet'):
                        # Adiciona o caminho S3 completo
                        s3_path = f"s3://{self.bucket_name}/{obj['Key']}"
                        parquet_files.append(s3_path)
                        print(f"Arquivo Parquet adicionado: {s3_path}")
            else:
                print(f"Nenhum objeto encontrado no prefixo: {s3_prefix}")
            
            if not parquet_files:
                print(f"Nenhum arquivo Parquet encontrado para {table_name}")
                return ""
            
            # Cria o conteúdo do manifest
            manifest_content = "\n".join(parquet_files)
            print(f"Conteúdo do manifest:\n{manifest_content}")
            
            # Salva o manifest no S3
            manifest_key = f"{s3_prefix}_symlink_format_manifest"
            print(f"Salvando manifest em: {manifest_key}")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=manifest_key,
                Body=manifest_content,
                ContentType='text/plain'
            )
            
            manifest_s3_path = f"s3://{self.bucket_name}/{manifest_key}"
            print(f"Symlink manifest gerado: {manifest_s3_path}")
            print(f"Arquivos referenciados: {len(parquet_files)}")
            
            return manifest_s3_path
            
        except Exception as e:
            print(f"Erro ao gerar symlink manifest: {str(e)}")
            import traceback
            traceback.print_exc()
            return ""
    
    def create_glue_table_from_delta(self, table_name: str, df: pd.DataFrame, s3_location: str) -> bool:
        """Cria tabela no AWS Glue Catalog baseada na estrutura Delta"""
        if not self.glue_client:
            print("Cliente Glue não configurado")
            return False
        
        try:
            print(f"Criando tabela Glue: {table_name}")
            print(f"Localização S3: {s3_location}")
            
            database_name = ATHENA_DATABASE
            print(f"Database: {database_name}")
            
            # Verifica se o database existe, se não cria
            try:
                self.glue_client.get_database(Name=database_name)
                print(f"Database '{database_name}' já existe")
            except self.glue_client.exceptions.EntityNotFoundException:
                print(f"Criando database '{database_name}'...")
                self.glue_client.create_database(
                    DatabaseInput={
                        'Name': database_name,
                        'Description': f'Database para tabelas Delta do ChatHib - {datetime.now().strftime("%Y-%m-%d")}'
                    }
                )
                print(f"Database '{database_name}' criado no Glue Catalog")
            
            # Converte schema do DataFrame para formato Glue
            columns = []
            for col_name in df.columns:
                dtype = str(df[col_name].dtype)
                glue_type = self.pandas_to_glue_type(dtype)
                
                columns.append({
                    'Name': col_name,
                    'Type': glue_type,
                    'Comment': f'Coluna {col_name} - tipo original: {dtype}'
                })
            
            print(f"Schema criado: {len(columns)} colunas")
            
            # Define parâmetros da tabela
            table_input = {
                'Name': table_name,
                'Description': f'Tabela Delta para {table_name} - Gerada automaticamente em {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {
                            'field.delim': '\t',
                            'serialization.format': '\t'
                        }
                    },
                    'Parameters': {
                        'classification': 'delta',
                        'delta.compatibility.symlinkFormatManifest.enabled': 'true',
                        'has_encrypted_data': 'false'
                    }
                },
                'PartitionKeys': [],
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'delta',
                    'delta.compatibility.symlinkFormatManifest.enabled': 'true',
                    'projection.enabled': 'false',
                    'storage.location.template': s3_location,
                    'transient_lastDdlTime': str(int(datetime.now().timestamp()))
                }
            }
            
            # Tenta criar a tabela (ou atualizar se já existe)
            try:
                print(f"Tentando criar tabela '{table_name}'...")
                self.glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                print(f"Tabela '{table_name}' criada no Glue Catalog")
                
            except self.glue_client.exceptions.AlreadyExistsException:
                # Tabela já existe, atualiza
                print(f"Tabela '{table_name}' já existe, atualizando...")
                self.glue_client.update_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                print(f"Tabela '{table_name}' atualizada no Glue Catalog")
            
            return True
            
        except Exception as e:
            print(f"Erro ao criar tabela no Glue Catalog: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def pandas_to_glue_type(self, pandas_dtype: str) -> str:
        """Converte tipos pandas para tipos compatíveis com Glue/Athena"""
        dtype_mapping = {
            'int64': 'bigint',
            'int32': 'int',
            'float64': 'double',
            'float32': 'float',
            'object': 'string',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp',
            'category': 'string'
        }
        
        return dtype_mapping.get(pandas_dtype, 'string')
    
    def setup_athena_compatibility(self, table_name: str, df: pd.DataFrame, s3_delta_path: str) -> Dict[str, str]:
        """Configura compatibilidade completa com Athena para tabela Delta"""
        print(f"Configurando compatibilidade Athena para tabela '{table_name}'...")
        print(f"S3 Delta Path: {s3_delta_path}")
        
        results = {}
        
        try:
            # 1. Gera symlink format manifest
            print("1Iniciando geração do symlink manifest...")
            manifest_path = self.generate_symlink_format_manifest("", table_name)
            if manifest_path:
                results['symlink_manifest'] = manifest_path
                print(f"Symlink manifest criado: {manifest_path}")
            else:
                print("Falha na criação do symlink manifest")
            
            # 2. Cria tabela no Glue Catalog
            print("Iniciando criação da tabela no Glue Catalog...")
            glue_success = self.create_glue_table_from_delta(table_name, df, s3_delta_path)
            if glue_success:
                results['glue_table'] = f"{ATHENA_DATABASE}.{table_name}"
                print(f"abela Glue criada: {ATHENA_DATABASE}.{table_name}")
            else:
                print("Falha na criação da tabela no Glue Catalog")
            
            # 3. Gera comando MSCK REPAIR TABLE
            msck_command = f"MSCK REPAIR TABLE {ATHENA_DATABASE}.{table_name};"
            results['msck_command'] = msck_command
            
            # 4. Gera query de exemplo para Athena
            sample_query = f"""-- Query de exemplo para Athena
                                SELECT * 
                                FROM {ATHENA_DATABASE}.{table_name} 
                                LIMIT 10;

                                -- Para executar MSCK REPAIR (se necessário):
                                -- {msck_command}

                                -- Verificar partições (se aplicável):
                                -- SHOW PARTITIONS {ATHENA_DATABASE}.{table_name};
                            """
            results['sample_query'] = sample_query
            
            print(f"Compatibilidade Athena configurada para '{table_name}'")
            print(f"Componentes criados: {list(results.keys())}")
            
            return results
            
        except Exception as e:
            print(f"Erro ao configurar compatibilidade Athena: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    