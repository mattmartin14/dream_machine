from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import duckdb
import os
import glob
import re
from typing import List, Dict, Any
import pandas as pd


app = FastAPI(title="C2 Session Detail API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _machine_slug(machine: str) -> str:
    m = (machine or "").strip().lower()
    if m.startswith("row"):  # RowErg
        return "rowerg"
    if m.startswith("ski"):  # SkiErg
        return "skierg"
    return m or "rowerg"


def _extract_log_id_from_path(path: str) -> int:
    m = re.search(r"(\d+)\.csv$", os.path.basename(path))
    return int(m.group(1)) if m else -1


def _query_detail_for_file(file_path: str) -> List[Dict[str, Any]]:
    # Pure-SQL approach using exact quoted headers
    con = duckdb.connect(database=":memory:")
    sql = r'''
        select
            stroke_number,
            watts,
            pace_seconds,
            printf(
                '%d:%02d.%02d',
                CAST(floor(CAST(round(pace_seconds * 100) AS INTEGER) / 6000) AS INTEGER),
                CAST(floor((CAST(round(pace_seconds * 100) AS INTEGER) % 6000) / 100) AS INTEGER),
                CAST(CAST(round(pace_seconds * 100) AS INTEGER) % 100 AS INTEGER)
            ) as pace_mm_ss_ff
        from (
            select
                "Number" as stroke_number,
                "Watts" as watts,
                CASE
                  WHEN try_cast("Pace (seconds)" as DOUBLE) is null THEN NULL
                  WHEN try_cast("Pace (seconds)" as DOUBLE) < 10 THEN try_cast("Pace (seconds)" as DOUBLE) * 60
                  ELSE try_cast("Pace (seconds)" as DOUBLE)
                END as pace_seconds
            from read_csv_auto(?, filename=true)
            where "Number" is not null and "Watts" is not null
        ) s
        order by stroke_number
    '''
    df = con.execute(sql, [file_path]).df()
    points: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        sn = r.get("stroke_number")
        w = r.get("watts")
        psec = r.get("pace_seconds", None)
        pstr = r.get("pace_mm_ss_ff", None)
        if sn is None or w is None:
            continue
        if pd.isna(sn) or pd.isna(w):
            continue
        try:
            sn_i = int(sn)
            w_f = float(w)
        except Exception:
            continue
        out: Dict[str, Any] = {"stroke_number": sn_i, "watts": w_f}
        if psec is not None and not pd.isna(psec):
            try:
                out["pace_seconds"] = float(psec)
            except Exception:
                pass
        if pstr is not None and not pd.isna(pstr):
            try:
                out["pace_mm_ss_ff"] = str(pstr)
            except Exception:
                pass
        points.append(out)
    return points


@app.get("/api/ping")
def ping():
    return {"status": "ok"}


@app.get("/api/session_detail")
def session_detail(date: str = Query(..., description="YYYY-MM-DD"), machine: str = Query("RowErg")):
    base = os.path.expanduser("~/concept2/workouts")
    kind = _machine_slug(machine)
    pattern = os.path.join(base, f"{date}_{kind}_detail_workout_*.csv")
    candidates = glob.glob(pattern)
    if not candidates:
        return {"error": "No session file found for date/machine", "pattern": pattern}

    candidates.sort(key=_extract_log_id_from_path, reverse=True)
    file_path = candidates[0]
    log_id = _extract_log_id_from_path(file_path)

    try:
        rows = _query_detail_for_file(file_path)
    except Exception as e:
        return {"error": f"DuckDB query failed: {e}", "file": file_path}

    strokes = []
    for r in rows:
        if r.get("watts") is None:
            continue
        item = {
            "x": r.get("stroke_number"),
            "y": r.get("watts"),
        }
        # include pace fields when present
        if "pace_seconds" in r:
            item["pace_seconds"] = r.get("pace_seconds")
        if "pace_mm_ss_ff" in r:
            item["pace_mm_ss_ff"] = r.get("pace_mm_ss_ff")
        strokes.append(item)
    return {
        "log_id": log_id,
        "filename": file_path,
        "date": date,
        "machine": machine,
        "points": strokes,
    }


def main():
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
