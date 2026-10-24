"""Result validation helpers."""

from __future__ import annotations

from dataclasses import dataclass


class DataValidationError(ValueError):
    """Raised when ETL output fails a correctness check."""


@dataclass(frozen=True)
class DailyResult:
    order_date: str
    order_count: int
    line_item_count: int
    gross_revenue: float
    discount_amount: float
    net_revenue: float
    average_order_value: float


def validate_daily_result(result: DailyResult) -> None:
    if result.order_count < 0 or result.line_item_count < 0:
        raise DataValidationError("Row counts cannot be negative")
    if result.line_item_count < result.order_count:
        raise DataValidationError("Line-item count cannot be lower than order count")
    if result.gross_revenue < 0 or result.discount_amount < 0 or result.net_revenue < 0:
        raise DataValidationError("Revenue metrics cannot be negative")
    if result.discount_amount > result.gross_revenue:
        raise DataValidationError("Discount amount cannot exceed gross revenue")
    expected_net = result.gross_revenue - result.discount_amount
    if abs(result.net_revenue - expected_net) > max(0.01, abs(expected_net) * 1e-8):
        raise DataValidationError("Net revenue does not equal gross revenue less discount")
    expected_aov = result.net_revenue / result.order_count if result.order_count else 0.0
    if abs(result.average_order_value - expected_aov) > max(0.01, abs(expected_aov) * 1e-8):
        raise DataValidationError("Average order value does not match net revenue / orders")
