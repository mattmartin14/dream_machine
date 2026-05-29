import json
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ----------------------------------------
# Config
# ----------------------------------------

MAX_WORKERS = 8
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 0.75
OUTPUT_DIR = Path(__file__).resolve().parent / "census_data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Census API endpoint
BASE_URL = (
    "https://api.census.gov/data/2020/dec/pl"
)
API_KEY = os.getenv("CENSUS_API_KEY")

# US state FIPS codes
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}

# ----------------------------------------
# Worker Function
# ----------------------------------------

def fetch_state_population(item):
    state_abbr, fips = item

    params = {
        "get": "NAME,P1_001N",
        "for": f"state:{fips}",
        "key": API_KEY,
    }

    try:
        data = None

        for attempt in range(1, MAX_RETRIES + 1):
            response = requests.get(BASE_URL, params=params, timeout=15)

            # Retry common transient statuses and server errors.
            if response.status_code in (408, 429, 500, 502, 503, 504):
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                    continue

            response.raise_for_status()

            try:
                data = response.json()
                break
            except ValueError:
                # Sometimes API returns HTML error pages with HTTP 200.
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                    continue
                snippet = response.text[:200].replace("\n", " ")
                raise ValueError(
                    f"Non-JSON response after retries: {snippet}"
                )

        if data is None:
            raise RuntimeError("No data returned from Census API after retries")

        # Census API format:
        # [
        #   ["NAME","P1_001N","state"],
        #   ["Georgia","10711908","13"]
        # ]

        if len(data) < 2:
            raise ValueError(f"Unexpected Census API response format: {data}")

        headers = data[0]
        values = data[1]

        result = dict(zip(headers, values))

        output = {
            "state_abbr": state_abbr,
            "state_name": result["NAME"],
            "population": int(result["P1_001N"]),
            "fips": result["state"],
            "fetched_at": time.time(),
        }

        # Write JSON file
        output_file = OUTPUT_DIR / f"{state_abbr}.json"

        with output_file.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        return {
            "state": state_abbr,
            "success": True,
            "population": output["population"],
        }

    except Exception as e:
        return {
            "state": state_abbr,
            "success": False,
            "error": str(e),
        }

# ----------------------------------------
# Main
# ----------------------------------------

def main():
    if not API_KEY:
        raise RuntimeError(
            "Missing CENSUS_API_KEY environment variable. "
            "Set it before running this script."
        )

    start = time.time()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(fetch_state_population, item)
            for item in STATE_FIPS.items()
        ]

        for future in as_completed(futures):
            result = future.result()

            results.append(result)

            if result["success"]:
                print(
                    f"[SUCCESS] "
                    f'{result["state"]}: '
                    f'{result["population"]:,}'
                )
            else:
                print(
                    f"[FAILED] "
                    f'{result["state"]}: '
                    f'{result["error"]}'
                )

    elapsed = time.time() - start

    successful = sum(1 for r in results if r["success"])
    failed = [r for r in results if not r["success"]]

    print("\n--------------------------------")
    print(f"Completed in {elapsed:.2f} seconds")
    print(f"Successful requests: {successful}")
    print(f"Failed requests: {len(failed)}")
    print(f"JSON files written to: {OUTPUT_DIR}")

    if failed:
        print("Sample failures:")
        for item in failed[:5]:
            print(f"  - {item['state']}: {item['error']}")

    print("--------------------------------")


if __name__ == "__main__":
    main()