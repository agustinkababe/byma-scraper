# byma_source.py
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime
import pandas as pd
import httpx
import asyncio
import io
import logging

from auth import get_current_user

router = APIRouter()

GENERAL_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general"
COTIZACION_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/cotizacion"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def fetch_data(symbol: str, client: httpx.AsyncClient):
    headers = {"Content-Type": "application/json"}
    general_payload = {"symbol": symbol}
    cotizacion_payload = {"symbol": symbol, "settlementType": "2"}

    forma_amortizacion = ""
    interes = ""
    fecha_emision = ""
    trade = "N/A"

    try:
        general_response = await client.post(GENERAL_URL, json=general_payload, headers=headers)
        general_response.raise_for_status()
        general_data = general_response.json()["data"][0]
        forma_amortizacion = general_data.get("formaAmortizacion", "")
        interes = general_data.get("interes", "")
        fecha_emision = general_data.get("fechaEmision", "")
    except Exception as e:
        logging.warning(f"[{symbol}] Error al obtener datos generales: {e}")

    for attempt in range(1, 1001):
        try:
            cotizacion_response = await client.post(COTIZACION_URL, json=cotizacion_payload, headers=headers)
            cotizacion_response.raise_for_status()
            cotizacion_data = cotizacion_response.json()["data"][0]
            raw_trade = cotizacion_data.get("trade", None)
            if raw_trade:
                trade = float(raw_trade) / 100  # ajustar con decimales
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 503:
                logging.warning(f"[{symbol}] Intento {attempt}/1000 - API cotización devolvió 503. Reintentando en 1 segundo...")
                await asyncio.sleep(1)
                continue
            else:
                logging.error(f"[{symbol}] Error inesperado de status: {e}")
                break
        except Exception as e:
            logging.error(f"[{symbol}] Error general en cotización: {e}")
            break

    return {
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "symbol": symbol,
        "formaAmortizacion": forma_amortizacion,
        "interes": interes,
        "fechaEmision": fecha_emision,
        "trade": trade
    }

@router.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), username: str = Depends(get_current_user)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    symbols = df["symbol"].dropna().unique()

    logging.info(f"{username} inició proceso para {len(symbols)} símbolos.")

    results = []
    async with httpx.AsyncClient(timeout=10) as client:
        tasks = [fetch_data(symbol, client) for symbol in symbols]
        results = await asyncio.gather(*tasks)

    output_df = pd.DataFrame(results)
    output = io.StringIO()
    output_df.to_csv(output, index=False)
    output.seek(0)

    logging.info("Proceso finalizado correctamente.")
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=bonos_byma.csv"}
    )
