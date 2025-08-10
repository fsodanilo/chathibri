from fastapi import APIRouter, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from services.pdf_service import PDFService
from services.chat_service import ChatService
from services.db_service import DBService

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")

pdf_service = PDFService()
chat_service = ChatService()
db_service = DBService()

@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@router.post("/upload")
def upload_pdf(file: UploadFile = File(...)):
    return pdf_service.upload_pdf(file)

@router.get("/pdfs")
def list_pdfs():
    return pdf_service.list_pdfs()

@router.post("/ask")
def ask_question(question: str, pdf_name: str):
    return chat_service.ask_question(question, pdf_name)

@router.get("/chat-history")
def recent_chats():
    return db_service.recent_chats()

@router.post("/create-table")
def create_table_from_pdf(pdf_name: str = Form(...)):
    return db_service.create_table_from_pdf(pdf_name)