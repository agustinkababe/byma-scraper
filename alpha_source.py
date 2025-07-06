# alpha_source.py
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime
import pandas as pd
import httpx
import asyncio
import io
import logging

from main import get_current_user  # reutilizamos la autenticación del backend principal

router = APIRouter()

ALPHA_API_KEY = "QJYLXMEEIIEAA5MS"
ALPHA_URL = "https://www.alphavantage.co/query"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def fetch_intraday(symbol: str, client: httpx.AsyncClient):
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "apikey": ALPHA_API_KEY,
        "outputsize": "full"
    }

    try:
        response = await client.get(ALPHA_URL, params=params)
        response.raise_for_status()
        data = response.json()
        timeseries = data.get("Time Series (1min)", {})
        
        if not timeseries:
            raise ValueError("No hay datos para el símbolo")

        parsed = []
        for timestamp, values in timeseries.items():
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            parsed.append({
                "Symbol": symbol,
                "Fecha": dt.date(),
                "Hora": dt.time(),
                "Open": values["1. open"],
                "High": values["2. high"],
                "Low": values["3. low"],
                "Volume": values["5. volume"]
            })

        # filtrar solo la última fecha disponible
        if parsed:
            last_date = max([p["Fecha"] for p in parsed])
            return [p for p in parsed if p["Fecha"] == last_date]
        else:
            return []

    except Exception as e:
        logging.warning(f"[{symbol}] Error al obtener datos: {e}")
        return []

@router.post("/alpha-source")
async def alpha_source(file: UploadFile = File(...), username: str = Depends(get_current_user)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    symbols = df["symbol"].dropna().unique()

    all_rows = []
    async with httpx.AsyncClient(timeout=20) as client:
        tasks = [fetch_intraday(symbol, client) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        for r in results:
            all_rows.extend(r)

    if not all_rows:
        raise HTTPException(status_code=500, detail="No se pudo recuperar ningún dato.")

    output_df = pd.DataFrame(all_rows)
    output = io.StringIO()
    output_df.to_csv(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=alpha_intraday.csv"}
    )
