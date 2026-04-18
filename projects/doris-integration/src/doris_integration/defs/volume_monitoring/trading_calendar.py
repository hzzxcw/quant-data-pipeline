from __future__ import annotations

from datetime import date, timedelta

import dagster as dg


class TradingCalendarResource(dg.ConfigurableResource):
    holiday_dates: str = ""
    makeup_dates: str = ""

    def _parse_dates(self, date_str: str) -> set[date]:
        if not date_str.strip():
            return set()
        return {date.fromisoformat(d.strip()) for d in date_str.split(",") if d.strip()}

    @property
    def holidays(self) -> set[date]:
        return self._parse_dates(self.holiday_dates)

    @property
    def makeup_trading_days(self) -> set[date]:
        return self._parse_dates(self.makeup_dates)

    def is_trading_day(self, d: date) -> bool:
        if d in self.makeup_trading_days:
            return True
        if d.weekday() >= 5:
            return False
        if d in self.holidays:
            return False
        return True

    def last_n_trading_days(self, d: date, n: int) -> list[date]:
        result: list[date] = []
        current = d
        while len(result) < n:
            if self.is_trading_day(current):
                result.append(current)
            current = current - timedelta(days=1)
        return sorted(result)
