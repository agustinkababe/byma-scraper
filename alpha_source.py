from fastapi import APIRouter, UploadFile, File, Depends
from fastapi.responses import StreamingResponse
from auth import get_current_user
import pandas as pd
import httpx
import asyncio
import io
import logging
from datetime import datetime

router = APIRouter()

ALPHA_API_KEY = "QJYLXMEEIIEAA5MS"
ALPHA_URL = "https://www.alphavantage.co/query"

logging.basicConfig(level=logging.INFO)

async def fetch_alpha(symbol: str, client: httpx.AsyncClient):
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "apikey": ALPHA_API_KEY
    }

    try:
        response = await client.get(ALPHA_URL, params=params)
        response.raise_for_status()
        data = response.json()

        ts_key = next(k for k in data if "Time Series" in k)
        time_series = data[ts_key]

        result = []
        for timestamp, values in time_series.items():
            fecha, hora = timestamp.split()
            result.append({
                "Symbol": symbol,
                "Fecha": fecha,
                "Hora": hora,
                "Open": values["1. open"],
                "High": values["2. high"],
                "Low": values["3. low"],
                "Volume": values["5. volume"]
            })

        return result
    except Exception as e:
        logging.error(f"[{symbol}] Error en Alpha Vantage: {e}")
        return []

@router.post("/alpha-csv")
async def generate_alpha_csv(file: UploadFile = File(...), username: str = Depends(get_current_user)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    symbols = df["symbol"].dropna().unique()

    logging.info(f"{username} inició proceso Alpha para {len(symbols)} símbolos.")

    all_data = []
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [fetch_alpha(symbol, client) for symbol in symbols]
        results = await asyncio.gather(*tasks)

        for result in results:
            all_data.extend(result)

    output_df = pd.DataFrame(all_data)
    output = io.StringIO()
    output_df.to_csv(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=alpha_output.csv"}
    )
