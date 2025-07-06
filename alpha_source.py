from fastapi import APIRouter, UploadFile, File, Depends
from fastapi.responses import StreamingResponse
from auth import get_current_user
import pandas as pd
import io
import httpx
import asyncio
import logging
from datetime import datetime

router = APIRouter()

ALPHA_API_KEY = "QJYLXMEEIIEAA5MS"
ALPHA_API_URL = "https://www.alphavantage.co/query"

logging.basicConfig(level=logging.INFO)

async def fetch_alpha_data(symbol: str, client: httpx.AsyncClient):
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "apikey": ALPHA_API_KEY
    }

    try:
        response = await client.get(ALPHA_API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if "Note" in data or "Error Message" in data:
            logging.warning(f"[{symbol}] Error o límite alcanzado: {data}")
            return []

        time_series = data.get("Time Series (1min)")
        if not time_series:
            logging.warning(f"[{symbol}] No se encontró 'Time Series (1min)'. Respuesta: {data}")
            return []

        result = []
        for timestamp, values in time_series.items():
            date, time = timestamp.split()
            result.append({
                "Symbol": symbol,
                "Fecha": date,
                "Hora": time,
                "Open": values["1. open"],
                "High": values["2. high"],
                "Low": values["3. low"],
                "Volume": values["5. volume"]
            })

        return result

    except Exception as e:
        logging.error(f"[{symbol}] Error general: {e}")
        return []

@router.post("/alpha-csv")
async def alpha_csv(file: UploadFile = File(...), username: str = Depends(get_current_user)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8", errors="ignore")))
    symbols = df["symbol"].dropna().unique()

    logging.info(f"[{username}] inició alpha-csv con {len(symbols)} símbolos.")

    all_data = []
    async with httpx.AsyncClient(timeout=30) as client:
        for i, symbol in enumerate(symbols):
            result = await fetch_alpha_data(symbol, client)
            all_data.extend(result)

            # Si hacés muchas requests, esperá para evitar rate limit (5 por minuto)
            if i < len(symbols) - 1:
                await asyncio.sleep(12)

    if not all_data:
        logging.warning("No se generaron datos. Posible error con todos los símbolos.")

    output_df = pd.DataFrame(all_data)
    output = io.StringIO()
    output_df.to_csv(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=datos_alpha.csv"}
    )
