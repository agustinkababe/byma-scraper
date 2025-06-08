from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import StreamingResponse
import requests
import pandas as pd
import io
from datetime import datetime
import secrets
import certifi

app = FastAPI()

# Configuración de usuario y contraseña
USERNAME = "nachi"
PASSWORD = "Mumina1117!"

security = HTTPBasic()

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, USERNAME)
    correct_password = secrets.compare_digest(credentials.password, PASSWORD)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"}
        )

@app.get("/generate-csv")
async def generate_csv(credentials: HTTPBasicCredentials = Depends(authenticate)):
    url = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/public-bonds"

    body = {
        "T1": True,
        "T0": False,
        "Content-Type": "application/json, text/plain"
    }

    headers = {
        "Content-Type": "application/json",
        "Referer": "https://open.bymadata.com.ar/",
        "Origin": "https://open.bymadata.com.ar",
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.post(url, json=body, headers=headers, verify=certifi.where())

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos de BYMA: {response.status_code}")

    data = response.json().get("data", [])
    if not data:
        raise HTTPException(status_code=404, detail="No se encontraron datos.")

    rows = []
    for item in data:
        rows.append({
            "symbol": item.get("symbol"),
            "trade": item.get("trade"),
            "date": datetime.now().strftime('%Y-%m-%d')
        })

    df = pd.DataFrame(rows)

    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=bymadata.csv"}
    )
