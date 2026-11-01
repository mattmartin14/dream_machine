import pytest

from hardcharger_demo.validation import DailyResult, validate_daily_result


def result(**overrides: object) -> DailyResult:
    values: dict[str, object] = {
        "order_date": "2026-10-20",
        "order_count": 2,
        "line_item_count": 5,
        "gross_revenue": 100.0,
        "discount_amount": 10.0,
        "net_revenue": 90.0,
        "average_order_value": 45.0,
    }
    values.update(overrides)
    return DailyResult(**values)  # type: ignore[arg-type]


def test_valid_daily_metrics_pass() -> None:
    validate_daily_result(result())


@pytest.mark.parametrize(
    "overrides",
    [
        {"order_count": -1},
        {"line_item_count": 1},
        {"discount_amount": 101.0},
        {"net_revenue": 89.0},
        {"average_order_value": 44.0},
    ],
)
def test_invalid_daily_metrics_fail(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        validate_daily_result(result(**overrides))
