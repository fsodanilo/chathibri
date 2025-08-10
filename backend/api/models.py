from pydantic import BaseModel

class UploadResponse(BaseModel):
    message: str
    pdf_name: str

class QuestionRequest(BaseModel):
    question: str
    pdf_name: str

class QuestionResponse(BaseModel):
    question: str
    answer: str

class ChatHistoryResponse(BaseModel):
    pdf_name: str
    question: str
    answer: str

class TableCreationResponse(BaseModel):
    message: str