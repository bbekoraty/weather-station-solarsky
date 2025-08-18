from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from config import URL, TOKEN, ORG
from domain import InfluxDataBase

Influx = InfluxDataBase(URL, TOKEN, ORG)

router = APIRouter(
    prefix="/api/v1/content",
    tags=["Content"],
    responses={
        404: {"message": "Not Found"}
    }
)

@router.get("/query_data")
def query_data(
    date: str = Query(..., description="YYYY:MM:DD"),
    time: str = Query(..., description="HH:MM:SS"),
    token: str = Query(..., description="UUID")
):
    try:
        target_time = datetime.strptime(f"{date} {time}", "%Y:%m:%d %H:%M:%S")

        results = Influx.read_nearest(
            bucket="devices",
            token=token,
            target_time=target_time
        )

        return {
            "status": "success",
            "query_time": target_time.isoformat(),
            "data": results if results else None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
