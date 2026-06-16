from __future__ import annotations

import enum
from dataclasses import dataclass


class Severity(enum.Enum):
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AnomalyType(enum.Enum):
    VOLUME_DROP = "VOLUME_DROP"


@dataclass(frozen=True)
class VolumeRecord:
    """Single measurement of an asset's row count at a point in time."""

    asset_key: str
    timestamp: str  # ISO 8601
    row_count: int
    partition_key: str | None = None
    run_id: str | None = None

    def to_insert_dict(self) -> dict[str, str | int | None]:
        return {
            "asset_key": self.asset_key,
            "timestamp": self.timestamp,
            "row_count": self.row_count,
            "partition_key": self.partition_key,
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class AnomalyResult:
    """Result of anomaly detection for a single asset."""

    asset_key: str
    anomaly_type: AnomalyType
    severity: Severity
    observed_count: int
    expected_count: float
    deviation_pct: float
    is_trading_day: bool
    consecutive_confirmations: int
    suppressed: bool
    reason: str

    @property
    def passed(self) -> bool:
        return self.suppressed or self.severity == Severity.WARNING
