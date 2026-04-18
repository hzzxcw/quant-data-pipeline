from datetime import date

import pytest

from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)


@pytest.fixture
def calendar() -> TradingCalendarResource:
    return TradingCalendarResource(
        holiday_dates="2026-01-01,2026-01-28,2026-01-29,2026-01-30,2026-01-31,2026-02-01,2026-02-02,2026-02-03,2026-04-04,2026-04-05,2026-04-06,2026-05-01,2026-05-02,2026-05-03,2026-05-04,2026-05-05,2026-06-19,2026-06-20,2026-09-25,2026-10-01,2026-10-02,2026-10-03,2026-10-04,2026-10-05,2026-10-06,2026-10-07",
        makeup_dates="2026-02-08,2026-02-14,2026-09-27,2026-10-10",
    )


class TestIsTradingDay:
    def test_weekend_not_trading_day(self, calendar: TradingCalendarResource) -> None:
        saturday = date(2026, 3, 7)
        sunday = date(2026, 3, 8)
        assert calendar.is_trading_day(saturday) is False
        assert calendar.is_trading_day(sunday) is False

    def test_holiday_not_trading_day(self, calendar: TradingCalendarResource) -> None:
        new_year = date(2026, 1, 1)
        spring_festival = date(2026, 1, 28)
        national_day = date(2026, 10, 1)
        assert calendar.is_trading_day(new_year) is False
        assert calendar.is_trading_day(spring_festival) is False
        assert calendar.is_trading_day(national_day) is False

    def test_makeup_day_is_trading_day(self, calendar: TradingCalendarResource) -> None:
        saturday_makeup = date(2026, 2, 8)
        sunday_makeup = date(2026, 2, 14)
        assert calendar.is_trading_day(saturday_makeup) is True
        assert calendar.is_trading_day(sunday_makeup) is True

    def test_regular_weekday_is_trading_day(
        self, calendar: TradingCalendarResource
    ) -> None:
        regular_monday = date(2026, 3, 2)
        regular_friday = date(2026, 3, 6)
        assert calendar.is_trading_day(regular_monday) is True
        assert calendar.is_trading_day(regular_friday) is True

    def test_makeup_overrides_weekend_but_not_holiday(self) -> None:
        cal = TradingCalendarResource(
            holiday_dates="2026-01-01",
            makeup_dates="2026-01-03",
        )
        assert cal.is_trading_day(date(2026, 1, 3)) is True
        assert cal.is_trading_day(date(2026, 1, 1)) is False


class TestLastNTradingDays:
    def test_returns_n_trading_days(self, calendar: TradingCalendarResource) -> None:
        result = calendar.last_n_trading_days(date(2026, 3, 6), 5)
        assert len(result) == 5
        assert all(calendar.is_trading_day(d) for d in result)

    def test_results_are_sorted_ascending(
        self, calendar: TradingCalendarResource
    ) -> None:
        result = calendar.last_n_trading_days(date(2026, 3, 6), 5)
        assert result == sorted(result)

    def test_skips_weekends(self, calendar: TradingCalendarResource) -> None:
        friday = date(2026, 3, 6)
        result = calendar.last_n_trading_days(friday, 3)
        assert result[-1] == friday
        assert all(d.weekday() < 5 or d in calendar.makeup_trading_days for d in result)

    def test_skips_holidays(self, calendar: TradingCalendarResource) -> None:
        after_spring_festival = date(2026, 2, 4)
        result = calendar.last_n_trading_days(after_spring_festival, 3)
        for d in result:
            assert d not in calendar.holidays


class TestBoundaryDates:
    def test_empty_holiday_config(self) -> None:
        cal = TradingCalendarResource(holiday_dates="", makeup_dates="")
        assert cal.is_trading_day(date(2026, 3, 3)) is True
        assert cal.is_trading_day(date(2026, 3, 7)) is False

    def test_whitespace_robustness(self) -> None:
        cal = TradingCalendarResource(
            holiday_dates=" 2026-01-01 , 2026-01-02 ",
            makeup_dates=" 2026-01-03 ",
        )
        assert date(2026, 1, 1) in cal.holidays
        assert date(2026, 1, 2) in cal.holidays
        assert date(2026, 1, 3) in cal.makeup_trading_days

    def test_parse_dates_empty_string(self) -> None:
        cal = TradingCalendarResource(holiday_dates="", makeup_dates="")
        assert cal.holidays == set()
        assert cal.makeup_trading_days == set()
