import requests
import os
import re
import getpass
import argparse
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
LOGIN_URL = "https://log.concept2.com/login"
SEASON_URL = "https://log.concept2.com/season/2025"
BASE_URL = "https://log.concept2.com"
WORKOUTS_DIR = "workouts"
MAX_RETRIES = 3
RETRY_DELAY = 5 # seconds
# --- End Configuration ---

def get_workout_details_from_row(row):
    """Extracts workout details from a table row."""
    date_str = row.find("td").text.strip()
    workout_date = datetime.strptime(date_str, "%m/%d/%y").strftime("%Y-%m-%d")

    machine_type_tag = row.find("td", class_="hidden-xs")
    machine = machine_type_tag.text.strip().lower().replace(" ", "") if machine_type_tag else "unknown"

    link_tag = row.find("a", href=re.compile(r"/profile/\d+/log/\d+"))
    if not link_tag:
        return None
    
    workout_path = link_tag["href"]
    workout_id = workout_path.split("/")[-1]

    return {
        "date": workout_date,
        "machine": machine,
        "path": workout_path,
        "id": workout_id
    }

def download_workout(session, workout, profile_id):
    """Downloads a single workout CSV with retry logic."""
    filename = f"{workout['date']}_{workout['machine']}_workout_{workout['id']}.csv"
    csv_download_url = f"{BASE_URL}/profile/{profile_id}/log/{workout['id']}/export/csv"
    file_path = os.path.join(WORKOUTS_DIR, filename)

    for attempt in range(MAX_RETRIES):
        try:
            csv_response = session.get(csv_download_url)
            csv_response.raise_for_status()
            
            with open(file_path, "wb") as f:
                f.write(csv_response.content)
            
            return f"Successfully downloaded {filename}"
        except requests.exceptions.RequestException as e:
            if e.response and e.response.status_code == 404:
                return f"Failed to download {filename} (404 Not Found). Skipping."
            
            print(f"Attempt {attempt + 1} failed for {filename}. Retrying in {RETRY_DELAY}s... Error: {e}")
            time.sleep(RETRY_DELAY)
    
    return f"Failed to download {filename} after {MAX_RETRIES} attempts."

def main():
    """
    Main function to handle login, scraping, and downloading of workout CSVs.
    """
    parser = argparse.ArgumentParser(description="Download Concept2 workout logs.")
    parser.add_argument("--limit", type=int, help="Limit the number of workouts to download.")
    parser.add_argument("--parallel-fetches", type=int, default=10, help="Number of parallel downloads.")
    args = parser.parse_args()

    if not os.path.exists(WORKOUTS_DIR):
        print(f"Creating directory: {WORKOUTS_DIR}")
        os.makedirs(WORKOUTS_DIR)

    with requests.Session() as session:
        username = input("Enter your Concept2 username: ")
        password = getpass.getpass("Enter your Concept2 password: ")

        print("\nLogging in...")
        login_payload = {"username": username, "password": password}
        
        try:
            response = session.post(LOGIN_URL, data=login_payload)
            response.raise_for_status()

            if "The username or password you entered is incorrect" in response.text:
                print("Login failed. Please check your username and password.")
                return
            print("Login successful.")

            all_workouts_url = f"{SEASON_URL}?per_page=all"
            print(f"Fetching all season data from {all_workouts_url}...")
            response = session.get(all_workouts_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            profile_link = soup.find("a", href=re.compile(r"/profile/(\d+)"))
            if not profile_link:
                print("Could not find profile ID. Page structure may have changed.")
                return
            profile_id = re.search(r"/profile/(\d+)", profile_link["href"]).group(1)
            print(f"Found profile ID: {profile_id}")

            log_table = soup.find("table", {"id": "log-table"})
            if not log_table:
                print("Could not find the workout log table.")
                return

            workout_rows = log_table.find("tbody").find_all("tr")
            if not workout_rows:
                print("No workout rows found in the table.")
                return

            workouts_to_download = []
            for row in workout_rows:
                details = get_workout_details_from_row(row)
                if details:
                    workouts_to_download.append(details)

            if args.limit:
                workouts_to_download = workouts_to_download[:args.limit]

            print(f"Found {len(workouts_to_download)} workouts. Starting download with {args.parallel_fetches} parallel workers...")

            with ThreadPoolExecutor(max_workers=args.parallel_fetches) as executor:
                futures = [executor.submit(download_workout, session, workout, profile_id) for workout in workouts_to_download]
                
                for future in as_completed(futures):
                    print(future.result())

            print(f"\nDownload process complete.")

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            print("Please check your internet connection and the website status.")

if __name__ == "__main__":
    main()



