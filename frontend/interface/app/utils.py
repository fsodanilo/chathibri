import json
import subprocess
from datetime import datetime, date
import os

from urllib.parse import quote_plus
from pymongo import MongoClient

def gerar_parametros(nome_pdf, title, author, edition, year, initial_page, discard):
    params = {
        "title": title,
        "author": author,
        "edition": edition,
        "ISBN": "0000000000",
        "year": year,
        "file": f"frontend/interface_rag/pdfs/{nome_pdf}",
        "path_in": "../",
        "path_out": "../",
        "initial_page": initial_page,
        "discard": discard,
        "jump_first_line": "False",
        "character_end_topic": " ",
        "can_jump_topic": "True",
        "local_bd": "./chroma_db",
        "collection": "books"
    }

    nome_json = os.path.splitext(nome_pdf)[0] + ".json" 
    #f"{nome_pdf.split('.')[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    caminho_json = f"../frontend/interface_rag/parametros/{nome_json}"
    with open(caminho_json, "w", encoding="utf-8") as f:
        json.dump(params, f, ensure_ascii=False, indent=2)

    return caminho_json

def executar_extracao(path_json):
    comando = ["python", "../arquitetura/extract_pdf_fe.py", "-j", path_json, "-o", "saida", "-r", "a"]
    resultado = subprocess.run(comando, capture_output=True, text=True)
    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)
    return resultado.stdout

def conecta_mongodb():
    try:
        user = os.getenv("MONGO_USER")
        password = os.getenv("MONGO_PASSWORD")
        encoded_password = quote_plus(password)
        mongo_chathib = os.getenv("MONGO_URI")

        MONGO_URI = f"mongodb+srv://bronx:{encoded_password}@{mongo_chathib}"

        client = MongoClient(MONGO_URI)

        return client
    except Exception as e:
        print("Erro ao conectar no MongoDB:", e)
        return None

def listar_uploads_mongo():
    # Conecta no cluster
    client = conecta_mongodb()

    # Banco de dados e coleção
    db = client["llm"]
    collection = db["pdfs"]
    
    return list(collection.find({}, {"_id": 0}))

def salvar_conversa(pergunta, resposta):
    try:
        client = conecta_mongodb()
        db = client["llm"]
        collection = db["chat_history"]

        registro = {
            "pergunta": pergunta,
            "resposta": resposta,
            "data": datetime.now()
        }

        collection.insert_one(registro)
        print("Pergunta/resposta salva no MongoDB.")
    except Exception as e:
        print("Erro ao salvar conversa:", e)

# def listar_conversas():
#     client = conecta_mongodb()
#     db = client["llm"]
#     collection = db["chat_history"]
#     return list(collection.find({}, {"_id": 0}))

def listar_conversas():
    client = conecta_mongodb()
    db = client["llm"]
    collection = db["chat_history"]
    # Inclui o _id para permitir feedback
    return list(collection.find({}).sort("data", -1).limit(5))



def listar_conversas_do_dia():
    client = conecta_mongodb()
    db = client["llm"]
    collection = db["chat_history"]

    hoje = date.today()
    inicio = datetime.combine(hoje, datetime.min.time())
    fim = datetime.combine(hoje, datetime.max.time())

    conversas = list(collection.find(
        {"data": {"$gte": inicio, "$lte": fim}}
    ).sort("data", 1))

    return conversas

