from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import duckdb
import os
import glob
import re
from typing import Any, Dict, List, Optional
import pandas as pd


app = FastAPI(title="C2 Dashboard API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

WORKOUTS_DIR = os.path.expanduser("~/concept2/workouts")
SUMMARY_GLOB = os.path.join(WORKOUTS_DIR, "*summary*.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def _machine_slug(machine: str) -> str:
    m = (machine or "").strip().lower()
    if m.startswith("row"):
        return "rowerg"
    if m.startswith("ski"):
        return "skierg"
    if m.startswith("bike"):
        return "bikeerg"
    return m or "rowerg"


def _extract_log_id_from_path(path: str) -> int:
    m = re.search(r"(\d+)\.csv$", os.path.basename(path))
    return int(m.group(1)) if m else -1


def _query_detail_for_file(file_path: str) -> List[Dict[str, Any]]:
    con = _con()
    sql = r'''
        select
            stroke_number,
            watts,
            stroke_rate,
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
                try_cast("Stroke Rate" as DOUBLE) as stroke_rate,
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
        sr = r.get("stroke_rate", None)
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
        if sr is not None and not pd.isna(sr):
            try:
                out["stroke_rate"] = float(sr)
            except Exception:
                pass
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


def _rows_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DataFrame to records, replacing NaN/NA with None."""
    records = df.to_dict(orient="records")
    cleaned = []
    for row in records:
        cleaned.append({
            k: None if (v is pd.NA or (isinstance(v, float) and pd.isna(v))) else v
            for k, v in row.items()
        })
    return cleaned


_PACE_CASE_SQL = """
    CASE
        WHEN lower("Type") IN ('rowerg', 'skierg')
        THEN SUM(TRY_CAST("Work Time (Seconds)" AS DOUBLE))
             / NULLIF(SUM(TRY_CAST("Work Distance" AS DOUBLE)) / 500.0, 0)
        ELSE SUM(TRY_CAST("Work Time (Seconds)" AS DOUBLE))
             / NULLIF(SUM(TRY_CAST("Work Distance" AS DOUBLE)) / 1000.0, 0)
    END
"""

_SESSION_SELECT = f"""
    SELECT
        strftime(TRY_CAST("Date" AS DATE), '%Y-%m-%d')                                        AS workout_date,
        "Type"                                                                                 AS machine,
        SUM(TRY_CAST("Work Distance" AS DOUBLE)) / 1000.0                                      AS total_distance_km,
        {_PACE_CASE_SQL}                                                                       AS avg_pace_seconds,
        AVG(TRY_CAST("Avg Watts" AS DOUBLE))                                                   AS avg_watts,
        SUM(TRY_CAST("Work Time (Seconds)" AS DOUBLE)) / 60.0                                  AS total_session_minutes,
        SUM(TRY_CAST("Stroke Count" AS DOUBLE))
            / NULLIF(SUM(TRY_CAST("Work Time (Seconds)" AS DOUBLE)) / 60.0, 0)                AS avg_spm
    FROM read_csv_auto('{SUMMARY_GLOB}')
"""

_SESSION_COLS = ["workout_date", "machine", "total_distance_km", "avg_pace_seconds",
                 "avg_watts", "total_session_minutes", "avg_spm"]


def _row_to_session(row) -> Dict[str, Any]:
    result = {}
    for k, v in zip(_SESSION_COLS, row):
        if isinstance(v, float) and pd.isna(v):
            result[k] = None
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Routes — existing (unchanged)
# ---------------------------------------------------------------------------

@app.get("/api/ping")
def ping():
    return {"status": "ok"}


@app.get("/api/session_detail")
def session_detail(
    date: str = Query(..., description="YYYY-MM-DD"),
    machine: str = Query("RowErg"),
):
    kind = _machine_slug(machine)
    pattern = os.path.join(WORKOUTS_DIR, f"{date}_{kind}_detail_workout_*.csv")
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
        item = {"x": r.get("stroke_number"), "y": r.get("watts")}
        if "pace_seconds" in r:
            item["pace_seconds"] = r["pace_seconds"]
        if "pace_mm_ss_ff" in r:
            item["pace_mm_ss_ff"] = r["pace_mm_ss_ff"]
        if "stroke_rate" in r:
            item["stroke_rate"] = r["stroke_rate"]
        strokes.append(item)

    return {"log_id": log_id, "filename": file_path, "date": date, "machine": machine, "points": strokes}


# ---------------------------------------------------------------------------
# Routes — new (replace Cube semantic layer)
# ---------------------------------------------------------------------------

@app.get("/api/daily_metrics")
def daily_metrics(
    machine: str = Query("RowErg"),
    start_date: Optional[str] = Query(None),
    distance_filter: bool = Query(False),
):
    pace_divisor = 500.0 if machine.lower().replace(" ", "") in ("rowerg", "skierg") else 1000.0
    params: List[Any] = [machine]
    where_parts = ['"Type" = ?', 'TRY_CAST("Work Time (Seconds)" AS DOUBLE) >= 300']

    if distance_filter:
        where_parts.append('TRY_CAST("Work Distance" AS DOUBLE) >= 5000')

    if start_date:
        where_parts.append('TRY_CAST("Date" AS DATE) >= TRY_CAST(? AS DATE)')
        params.append(start_date)

    where = " AND ".join(where_parts)
    sql = f"""
        SELECT
            strftime(TRY_CAST("Date" AS DATE), '%Y-%m-%d')                                    AS workout_date,
            AVG(TRY_CAST("Avg Watts" AS DOUBLE))                                               AS avg_watts,
            SUM(TRY_CAST("Stroke Count" AS DOUBLE))
                / NULLIF(SUM(TRY_CAST("Work Time (Seconds)" AS DOUBLE)) / 60.0, 0)            AS avg_spm,
            SUM(TRY_CAST("Work Time (Seconds)" AS DOUBLE))
                / NULLIF(SUM(TRY_CAST("Work Distance" AS DOUBLE)) / {pace_divisor}, 0)        AS avg_pace_seconds,
            AVG(TRY_CAST("Work Time (Seconds)" AS DOUBLE))                                     AS avg_session_duration_seconds,
            SUM(TRY_CAST("Work Distance" AS DOUBLE)) / 1000.0                                  AS total_distance_km
        FROM read_csv_auto('{SUMMARY_GLOB}')
        WHERE {where}
        GROUP BY strftime(TRY_CAST("Date" AS DATE), '%Y-%m-%d')
        ORDER BY workout_date
    """
    con = _con()
    df = con.execute(sql, params).df()
    return {"data": _rows_to_records(df)}


@app.get("/api/session_summary")
def session_summary(
    log_id: Optional[str] = Query(None),
    date: Optional[str] = Query(None),
    machine: Optional[str] = Query(None),
):
    con = _con()
    if log_id is not None:
        sql = (
            _SESSION_SELECT
            + ' WHERE TRY_CAST("Log ID" AS BIGINT) = TRY_CAST(? AS BIGINT)'
            + ' GROUP BY strftime(TRY_CAST("Date" AS DATE), \'%Y-%m-%d\'), "Type" LIMIT 1'
        )
        params: List[Any] = [log_id]
    elif date and machine:
        sql = (
            _SESSION_SELECT
            + ' WHERE strftime(TRY_CAST("Date" AS DATE), \'%Y-%m-%d\') = ? AND "Type" = ?'
            + ' GROUP BY strftime(TRY_CAST("Date" AS DATE), \'%Y-%m-%d\'), "Type" LIMIT 1'
        )
        params = [date, machine]
    else:
        return {"error": "Provide log_id or both date and machine"}

    row = con.execute(sql, params).fetchone()
    if not row:
        return {"error": "No session found"}
    return _row_to_session(row)


@app.get("/api/latest_workout")
def latest_workout(machine: str = Query("RowErg")):
    sql = (
        _SESSION_SELECT
        + ' WHERE "Type" = ?'
        + ' GROUP BY strftime(TRY_CAST("Date" AS DATE), \'%Y-%m-%d\'), "Type"'
        + ' ORDER BY workout_date DESC LIMIT 1'
    )
    con = _con()
    row = con.execute(sql, [machine]).fetchone()
    if not row:
        return {"error": "No workout found"}
    return _row_to_session(row)


@app.get("/api/monthly_distance")
def monthly_distance():
    sql = f"""
        SELECT
            strftime(TRY_CAST("Date" AS DATE), '%Y-%m')                                       AS month,
            "Type"                                                                             AS machine,
            SUM(
                TRY_CAST("Work Distance" AS DOUBLE)
                + COALESCE(TRY_CAST("Rest Distance" AS DOUBLE), 0)
            ) / 1000.0                                                                         AS total_distance_with_rest_km
        FROM read_csv_auto('{SUMMARY_GLOB}')
        WHERE "Type" IN ('RowErg', 'SkiErg', 'BikeErg')
        GROUP BY strftime(TRY_CAST("Date" AS DATE), '%Y-%m'), "Type"
        ORDER BY month, machine
    """
    con = _con()
    df = con.execute(sql).df()
    return {"data": _rows_to_records(df)}


@app.get("/api/lifetime_kpis")
def lifetime_kpis():
    sql = f"""
        SELECT
            "Type"                                                                             AS machine,
            SUM(
                TRY_CAST("Work Distance" AS DOUBLE)
                + COALESCE(TRY_CAST("Rest Distance" AS DOUBLE), 0)
            ) / 1000.0                                                                         AS total_distance_with_rest_km,
            {_PACE_CASE_SQL}                                                                   AS avg_pace_seconds
        FROM read_csv_auto('{SUMMARY_GLOB}')
        WHERE "Type" IN ('RowErg', 'SkiErg', 'BikeErg')
        GROUP BY "Type"
    """
    con = _con()
    df = con.execute(sql).df()
    return {"data": _rows_to_records(df)}


@app.get("/api/monthly_watts")
def monthly_watts(start_date: Optional[str] = Query(None)):
    params: List[Any] = []
    where_parts = ['"Type" IN (\'RowErg\', \'SkiErg\', \'BikeErg\')']

    if start_date:
        where_parts.append('TRY_CAST("Date" AS DATE) >= TRY_CAST(? AS DATE)')
        params.append(start_date)

    where = " AND ".join(where_parts)
    sql = f"""
        SELECT
            strftime(TRY_CAST("Date" AS DATE), '%Y-%m')                                       AS month,
            "Type"                                                                             AS machine,
            AVG(TRY_CAST("Avg Watts" AS DOUBLE))                                               AS avg_watts,
            AVG(TRY_CAST("Drag Factor" AS DOUBLE))                                             AS avg_drag_factor,
            SUM(TRY_CAST("Work Distance" AS DOUBLE)) / 1000.0                                  AS total_distance_km
        FROM read_csv_auto('{SUMMARY_GLOB}')
        WHERE {where}
        GROUP BY strftime(TRY_CAST("Date" AS DATE), '%Y-%m'), "Type"
        ORDER BY month, machine
    """
    con = _con()
    df = con.execute(sql, params).df()
    return {"data": _rows_to_records(df)}


def main():
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
