"""
Processor module for stock-backtest-alpha backtest signal.

This module validates incoming messages and computes a derived alpha signal
based on the input data. All operations are logged for observability.
"""

from typing import Any

from app.utils.setup_logger import setup_logger
from app.utils.types import ValidatedMessage
from app.utils.validate_data import validate_message_schema

logger = setup_logger(__name__)


def validate_input_message(message: dict[str, Any]) -> ValidatedMessage:
    """
    Validate the incoming raw message against the expected schema.

    Args:
        message (dict[str, Any]): The raw message payload.

    Returns:
        ValidatedMessage: A validated message object.

    Raises:
        ValueError: If the message format is invalid.
    """
    logger.debug("🔍 Validating message schema...")
    if not validate_message_schema(message):
        logger.error("❌ Invalid message schema: %s", message)
        raise ValueError("Invalid message format")
    return message  # type: ignore[return-value]


def compute_signal(message: ValidatedMessage) -> dict[str, Any]:
    """
    Compute an alpha backtest signal from the validated input message.

    This is a placeholder implementation. Replace this logic with the
    actual alpha model or rules-based strategy as needed.

    Args:
        message (ValidatedMessage): The validated input data.

    Returns:
        dict[str, Any]: Enriched message with the computed signal and metadata.
    """
    symbol = message.get("symbol", "UNKNOWN")
    timestamp = message.get("timestamp", "N/A")
    logger.info("⚙️ Computing signal for %s @ %s", symbol, timestamp)

    # Placeholder signal logic: always HOLD with 0.5 confidence
    signal_result = {
        "signal": "HOLD",
        "confidence": 0.5,
        "reason": "Default placeholder signal logic",
    }

    logger.debug("✅ Computed signal for %s: %s", symbol, signal_result)
    return {**message, **signal_result}
