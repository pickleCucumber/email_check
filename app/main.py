from fastapi import FastAPI, Depends, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from config import *
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, EmailStr
import json
from database import get_db
from utils import check_token
import logging
import redis
import os
import time
from dotenv import load_dotenv

load_dotenv()

redis_client = redis_client
QUEUE_NAME = "email_queue"
PROCESSING_NAME = "email_processing"
BOUNCE_PENDING = "hash_bounce_pending"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI()

def add_to_redis_queue(record_id: int):
    start = time.time()
    logger.info(f"[{start}] add_to_redis_queue START for {record_id}")
    task = {"record_id": record_id, "retries": 0}
    try:
        redis_client.rpush(QUEUE_NAME, json.dumps(task))
        end = time.time()
        logger.info(f"[{end}] add_to_redis_queue END for {record_id}, duration {end-start:.3f}s")
    except Exception as e:
        logger.error(f"Redis error for {record_id}: {e}")
# =========================
# Schema
# =========================
class EmailRequest(BaseModel):
    email: EmailStr
    subject: str
    body: str


# =========================
# Exception handlers
# =========================
#замер времени
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f"Request {request.method} {request.url.path} took {process_time:.3f}s")
    return response

# ошибки валидации Pydantic в 400
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Validation error: {exc.errors()}")
    return JSONResponse(
        status_code=400,
        content={
            "success": False,
            "message": "Ошибка при вызове метода: не переданы обязательные параметры или они некорректны"
        }
    )

#логируем все HTTP исключения
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(f"HTTP {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.detail if isinstance(exc.detail, dict) else {"success": False, "message": exc.detail}
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(f"Internal server error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "Внутренняя ошибка сервиса"
        }
    )


# =========================
# POST /send
# =========================
@app.post("/send")
def send_email(
    payload: EmailRequest,
    authorization: str = Header(..., alias="Authorization"),
    db: Session = Depends(get_db)
):
    request_start = time.time()
    logger.info(f"[{request_start}] /send START")


    # проверка токена (401 при ошибке)
    check_token(authorization)

    # проверка дубля
    start = time.time()
    # проверка дубля (400, если почта уже в очереди)
    query = text("""
        SELECT TOP 1 Id
        FROM EmailCheckQueue WITH (NOLOCK)
        WHERE Email = :email AND Status='in_queue'
    """)
    existing = db.execute(query, {"email": payload.email}).fetchone()
    logger.info(f"2. Duplicate check took {time.time()-start:.3f}s")

    if existing:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Указанная почта уже находится в очереди на проверку"
            }
        )
    # вставка в очередь
    start = time.time()
    insert_query = text("""
        INSERT INTO EmailCheckQueue ([Subject], Email, Body, [Status], IsSent)
        OUTPUT inserted.Id 
        VALUES (:subject, :email, :body, 'in_queue', 0);
    """)
    insert_start = time.time()
    result = db.execute(insert_query, {
        "email": payload.email,
        "subject": payload.subject,
        "body": payload.body
    })
    row = result.fetchone()
    db.commit()
    commit_end = time.time()
    logger.info(f"[{commit_end}] DB commit done for {row.Id}, duration {commit_end - insert_start:.3f}s")
    redis_start = time.time()
    add_to_redis_queue(row.Id)
    logger.info(f"Redis enqueue took {time.time() - redis_start:.3f}s")
    logger.info(f"[{time.time()}] /send END, total duration {time.time() - request_start:.3f}s")
    return {"success": True, "queueId": str(row.Id)}



# =========================
# GET
# =========================

@app.get("/status")
def get_status(
    queueId: str = None,
    authorization: str = Header(..., alias="Authorization"),
    db: Session = Depends(get_db)
):
    #  проверка токена (401 при ошибке)
    check_token(authorization)

    #  проверка наличия queueId (400)
    if not queueId:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "не все обязательные параметры переданы"
            }
        )

    #  поиск записи в очереди
    query = text("""
        SELECT Status, DtSending, Reason
        FROM EmailCheckQueue WITH (NOLOCK)
        WHERE Id = :id
    """)
    row = db.execute(query, {"id": queueId}).fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail={
                "success": False,
                "message": "в очереди не найдена указанная запись"
            }
        )

    return {
        "success": True,
        "status": row.Status,
        "dtSending": row.DtSending,
        "reason": row.Reason
    }