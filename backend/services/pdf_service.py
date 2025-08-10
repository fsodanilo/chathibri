import fitz
from backend.services.mongo import db

class PDFService:
    def __init__(self):
        self.db = db
        self.collection = self.db["pdfs"]

    def upload_pdf(self, file):
        contents = file.file.read()
        doc = fitz.open(stream=contents, filetype="pdf")
        text = "".join(page.get_text() for page in doc)

        self.collection.insert_one({"name": file.filename, "content": text})
        return {"message": "PDF enviado com sucesso!", "pdf_name": file.filename}

    def list_pdfs(self):
        pdfs = self.collection.find({}, {"_id": 0, "name": 1})
        return [pdf["name"] for pdf in pdfs]