from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from auth.oauth import oauth
from pymongo import MongoClient
from urllib.parse import quote_plus
import os

router = APIRouter()

usuario = os.getenv("MONGO_USER")
senha = os.getenv("MONGO_PASSWORD")
encoded_senha = quote_plus(senha)
mongo_chathib = os.getenv("MONGO_CHATHIB")

# Substitua pelos seus dados reais
MONGO_URI = f"mongodb+srv://{usuario}:{encoded_senha}@{mongo_chathib}"


# Conexão com MongoDB
client = MongoClient(MONGO_URI)
db = client.get_default_database()
users_collection = db["users"]

@router.get("/login")
async def login(request: Request):
    redirect_uri = request.url_for("auth")
    return await oauth.google.authorize_redirect(request, redirect_uri)

@router.get("/auth")
async def auth(request: Request):
    token = await oauth.google.authorize_access_token(request)
    # user_info = await oauth.google.parse_id_token(request, token)

    user_info = token.get("userinfo")

    if not user_info:
        # fallback para parse_id_token (caso esteja disponível)
        try:
            user_info = await oauth.google.parse_id_token(request, token)
        except Exception:
            raise HTTPException(status_code=400, detail="Não foi possível obter os dados do usuário.")


    existing_user = users_collection.find_one({"sub": user_info["sub"]})
    if not existing_user:
        users_collection.insert_one({
            "sub": user_info["sub"],
            "name": user_info["name"],
            "email": user_info["email"],
            "picture": user_info.get("picture")
        })

    request.session["user"] = {
        "name": user_info["name"],
        "email": user_info["email"],
        "picture": user_info.get("picture")
    }
    return RedirectResponse(url="/")

@router.get("/logout")
async def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(url="/")