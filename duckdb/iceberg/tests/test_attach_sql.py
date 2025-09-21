import re
from scripts.attach_biglake import attempt_attach

class DummyCon:
    def __init__(self):
        self.executed = []
    def execute(self, sql):
        self.executed.append(sql)
        # Simulate failure to allow inspection
        raise Exception("Simulated network failure")

def test_attempt_attach_builds_sql(monkeypatch):
    con = DummyCon()
    called = {}
    def fake_execute(sql):
        called['sql'] = sql
        raise Exception('boom')
    con.execute = fake_execute  # type: ignore
    ok, err = attempt_attach(con, "https://example.com/iceberg/v1beta/restcatalog", "tok", "gs://bucket/path")
    assert not ok
    assert 'boom' in err
    sql = called['sql']
    assert "ATTACH 'gs://bucket/path' AS biglake" in sql
    assert "ENDPOINT 'https://example.com/iceberg/v1beta/restcatalog'" in sql
    assert "TOKEN 'tok'" in sql
    # Ensure no accidental newlines modify structure
    assert re.search(r"ATTACH 'gs://bucket/path' AS biglake \\\(", sql) is None
