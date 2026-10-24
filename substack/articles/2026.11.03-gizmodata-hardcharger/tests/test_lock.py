from hardcharger_demo.locking import EtlLock


class FakeClient:
    class Exceptions:
        class ConditionalCheckFailedException(Exception):
            pass

    exceptions = Exceptions()

    def __init__(self) -> None:
        self.put: dict[str, object] | None = None
        self.delete: dict[str, object] | None = None

    def put_item(self, **kwargs: object) -> None:
        self.put = kwargs

    def delete_item(self, **kwargs: object) -> None:
        self.delete = kwargs


def test_etl_lock_is_conditionally_acquired_and_owner_released() -> None:
    client = FakeClient()
    with EtlLock("lock-table", "run-123", client=client):
        assert client.put is not None
        assert client.put["ConditionExpression"] == "attribute_not_exists(lock_key)"
    assert client.delete is not None
    assert client.delete["ExpressionAttributeValues"] == {":owner": {"S": "run-123"}}
