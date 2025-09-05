import requests
import os
import re
import getpass
import argparse
import time
import csv
import io
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
LOGIN_URL = "https://log.concept2.com/login"
BASE_URL = "https://log.concept2.com"
# The script will save workouts to the user's home directory.
WORKOUTS_DIR = os.path.expanduser("~/concept2/workouts")
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

def download_season_summary(session, season_year):
    """Downloads the season summary CSV file and adds the season year as a column."""
    summary_url = f"https://log.concept2.com/season/{season_year}/export"
    filename = f"season_summary_{season_year}.csv"
    file_path = os.path.join(WORKOUTS_DIR, filename)

    try:
        response = session.get(summary_url)
        response.raise_for_status()

        # Read the CSV content into memory
        csv_content = response.content.decode('utf-8')
        reader = csv.reader(io.StringIO(csv_content))
        rows = list(reader)

        # Add new header and data
        if rows:
            headers = rows[0]
            headers.append('season')
            for i in range(1, len(rows)):
                rows[i].append(season_year)

        # Write the modified CSV content back to a file
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(rows)
        
        with open(file_path, "w", newline="") as f:
            f.write(output.getvalue())

        return f"Successfully downloaded and modified season summary to {file_path}"
    except requests.exceptions.RequestException as e:
        return f"Failed to download season summary. Error: {e}"
    except csv.Error as e:
        return f"Failed to process season summary CSV. Error: {e}"


def download_workout(session, workout, profile_id, season_year):
    """Downloads a single workout CSV with retry logic. Only downloads if file doesn't exist."""
    filename = f"{workout['date']}_{workout['machine']}_detail_workout_{workout['id']}.csv"
    csv_download_url = f"{BASE_URL}/profile/{profile_id}/log/{workout['id']}/export/csv"
    file_path = os.path.join(WORKOUTS_DIR, filename)

    # Check if file already exists
    if os.path.exists(file_path):
        return {"status": "skipped", "message": f"Skipped {filename} (already exists)"}

    for attempt in range(MAX_RETRIES):
        try:
            csv_response = session.get(csv_download_url)
            csv_response.raise_for_status()

            # Read the CSV content into memory
            csv_content = csv_response.content.decode('utf-8')
            reader = csv.reader(io.StringIO(csv_content))
            rows = list(reader)

            # Add new headers and data
            headers = rows[0]
            headers.extend(['season', 'date', 'machine_type'])
            
            for i in range(1, len(rows)):
                rows[i].extend([season_year, workout['date'], workout['machine']])

            # Write the modified CSV content back to the file
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerows(rows)
            
            with open(file_path, "w", newline="") as f:
                f.write(output.getvalue())
            
            return {"status": "downloaded", "message": f"Successfully downloaded {filename}"}
        except requests.exceptions.RequestException as e:
            # If a 404 error occurs, it means no detailed CSV is available.
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
                return {"status": "no_data", "message": f"No detailed CSV available for workout {workout['id']}."}
            
            print(f"Attempt {attempt + 1} failed for {filename}. Retrying in {RETRY_DELAY}s... Error: {e}")
            time.sleep(RETRY_DELAY)
    
    return {"status": "failed", "message": f"Failed to download {filename} after {MAX_RETRIES} attempts."}


def main():
    """
    Main function to handle login, scraping, and downloading of workout CSVs.
    """
    parser = argparse.ArgumentParser(description="Download Concept2 workout logs.")
    parser.add_argument("--limit", type=int, help="Limit the number of workouts to download.")
    parser.add_argument("--parallel-fetches", type=int, default=10, help="Number of parallel downloads.")
    parser.add_argument("--season-year", type=int, default=2024, help="The season year to download workouts from.")
    args = parser.parse_args()

    if not os.path.exists(WORKOUTS_DIR):
        print(f"Creating directory: {WORKOUTS_DIR}")
        os.makedirs(WORKOUTS_DIR)

    with requests.Session() as session:
        #username = input("Enter your Concept2 username: ")
        #password = getpass.getpass("Enter your Concept2 password: ")
        username = os.getenv("C2_LOG_USERNAME")
        password = os.getenv("C2_LOG_PASSWORD")

        print("\nLogging in...")
        login_payload = {"username": username, "password": password}
        
        try:
            response = session.post(LOGIN_URL, data=login_payload)
            response.raise_for_status()

            if "The username or password you entered is incorrect" in response.text:
                print("Login failed. Please check your username and password.")
                return
            print("Login successful.")

            # Download the season summary
            summary_result = download_season_summary(session, args.season_year)
            print(summary_result)

            season_url = f"https://log.concept2.com/season/{args.season_year}"
            all_workouts_url = f"{season_url}?per_page=all"
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

            # Track download statistics
            new_downloads = 0
            skipped_files = 0
            failed_downloads = 0
            no_data_count = 0

            with ThreadPoolExecutor(max_workers=args.parallel_fetches) as executor:
                futures = [executor.submit(download_workout, session, workout, profile_id, args.season_year) for workout in workouts_to_download]
                
                for future in as_completed(futures):
                    result = future.result()
                    print(result["message"])
                    
                    # Count the results
                    if result["status"] == "downloaded":
                        new_downloads += 1
                    elif result["status"] == "skipped":
                        skipped_files += 1
                    elif result["status"] == "failed":
                        failed_downloads += 1
                    elif result["status"] == "no_data":
                        no_data_count += 1

            # Print summary statistics
            print(f"\n--- Download Summary ---")
            print(f"Total workouts found: {len(workouts_to_download)}")
            print(f"New detail files downloaded: {new_downloads}")
            print(f"Files skipped (already exist): {skipped_files}")
            print(f"Workouts with no detail data: {no_data_count}")
            print(f"Failed downloads: {failed_downloads}")
            print(f"\nDownload process complete. All files are saved in '{WORKOUTS_DIR}'.")

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            print("Please check your internet connection and the website status.")

if __name__ == "__main__":
    main()
