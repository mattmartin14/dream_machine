import duckdb
import google.auth
import google.auth.transport.requests

# NOTE: This helper is intentionally minimal. We add a small convenience method
# to produce standard REST headers for the BigLake Iceberg REST catalog so that
# debugging raw HTTP endpoints (outside of DuckDB) can share logic.
class BigLakeAuthHelper:
    def __init__(self, con: duckdb.DuckDBPyConnection, secret_name="biglake_oauth"):
        self.con = con
        self.secret_name = secret_name
        self.credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.request = google.auth.transport.requests.Request()
    def get_token(self) -> str:
        if not self.credentials.valid:
            self.credentials.refresh(self.request)
            print(self.credentials)
        return self.credentials.token
    def get_headers(self, project_id: str | None = None) -> dict:
        """Return standard headers for BigLake Iceberg REST API calls.

        Parameters
        ----------
        project_id: Optional GCP project id. If provided, include x-goog-user-project
            which some Google APIs require for proper billing attribution.
        """
        token = self.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if project_id:
            headers["x-goog-user-project"] = project_id
        return headers
    
    def create_or_update_secret(self):
        token = self.get_token()
        self.con.execute(f"DROP SECRET IF EXISTS {self.secret_name}")
        self.con.execute(f"""
        CREATE SECRET {self.secret_name} (
        TYPE GCS,
        TOKEN '{token}'
        );
        """)
        print("■ DuckDB secret refreshed")