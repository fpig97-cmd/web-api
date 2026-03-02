from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import sqlite3
import csv
import io

app = FastAPI()


# ----- SQLite 연결 & 테이블 생성 -----
conn = sqlite3.connect("logs.db", check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS logs(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id   INTEGER,
    user_id    INTEGER,
    action     TEXT,
    detail     TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()


# ----- 요청 바디 스키마 -----
class LogItem(BaseModel):
    guild_id: int
    user_id: int
    action: str
    detail: str


# ----- 봇이 로그 보내는 엔드포인트 (POST) -----
@app.post("/api/log")
def create_log(item: LogItem):
    cur.execute(
        "INSERT INTO logs (guild_id, user_id, action, detail) VALUES (?, ?, ?, ?)",
        (item.guild_id, item.user_id, item.action, item.detail),
    )
    conn.commit()
    return {"ok": True}


# ----- 인증 성공 로그 조회 (JSON) -----
@app.get("/api/logs/verify")
def get_verify_logs(
    guild_id: int | None = None,
    user_id: int | None = None,
    limit: int = 50,
):
    query = """
    SELECT guild_id, user_id, action, detail, created_at
    FROM logs
    WHERE action = 'verify_success'
    """
    params: list = []

    if guild_id is not None:
        query += " AND guild_id = ?"
        params.append(guild_id)

    if user_id is not None:
        query += " AND user_id = ?"
        params.append(user_id)

    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(query, tuple(params)).fetchall()

    return [
        {
            "guild_id": r[0],
            "user_id": r[1],
            "action": r[2],
            "detail": r[3],
            "created_at": r[4],
        }
        for r in rows
    ]


# ----- 인증 성공 로그 CSV 다운로드 -----
@app.get("/api/logs/verify.csv")
def download_verify_logs_csv(
    guild_id: int | None = None,
    user_id: int | None = None,
    limit: int = 100,
):
    query = """
    SELECT guild_id, user_id, action, detail, created_at
    FROM logs
    WHERE action = 'verify_success'
    """
    params: list = []

    if guild_id is not None:
        query += " AND guild_id = ?"
        params.append(guild_id)

    if user_id is not None:
        query += " AND user_id = ?"
        params.append(user_id)

    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(query, tuple(params)).fetchall()

    # 메모리 상에 CSV 작성
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["guild_id", "user_id", "action", "detail", "created_at"])
    for r in rows:
        writer.writerow([
            str(r[0]),          # guild_id를 문자열로
            str(r[1]),          # user_id를 문자열로
            r[2],
            r[3],
        str(r[4]),
        ])


    output.seek(0)

    headers = {
        "Content-Disposition": 'attachment; filename="verify_logs.csv"'
    }

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers=headers,
    )
