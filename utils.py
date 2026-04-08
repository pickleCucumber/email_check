from fastapi import HTTPException
import os
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")


def check_token(token: str):
    if not token:
        raise HTTPException(
            status_code=401,
            detail={"success": False, "message": "токен не передан"}
        )

    if token != API_TOKEN:
        raise HTTPException(
            status_code=401,
            detail={"success": False, "message": "некорректный токен"}
        )