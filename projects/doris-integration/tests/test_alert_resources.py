import logging

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from doris_integration.defs.volume_monitoring.models import (
    AnomalyResult,
    AnomalyType,
    Severity,
)
from doris_integration.defs.volume_monitoring.resources import (
    DingTalkAlertResource,
    WebhookAlertResource,
)


class TestDingTalkAlertResource:
    def test_mock_mode_logs_when_no_url(self, caplog: pytest.LogCaptureFixture) -> None:
        resource = DingTalkAlertResource()
        with caplog.at_level(logging.INFO):
            resource.send("Test Alert", "Test content", Severity.ERROR)
        assert "[MOCK]" in caplog.text

    def test_mock_mode_with_mention_all(self, caplog: pytest.LogCaptureFixture) -> None:
        resource = DingTalkAlertResource(mention_all=True)
        with caplog.at_level(logging.INFO):
            resource.send("Critical Alert", "Data missing", Severity.CRITICAL)
        assert "[MOCK]" in caplog.text

    @patch("urllib.request.urlopen")
    def test_real_mode_sends_request(self, mock_urlopen: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        resource = DingTalkAlertResource(webhook_url="https://example.com/webhook")
        resource.send("Alert", "Content", Severity.WARNING)

        mock_urlopen.assert_called_once()
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        assert req.method == "POST"
        assert "example.com" in req.full_url

    @patch("urllib.request.urlopen")
    def test_real_mode_raises_on_failure(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = Exception("Connection refused")

        resource = DingTalkAlertResource(webhook_url="https://example.com/webhook")
        with pytest.raises(Exception, match="Connection refused"):
            resource.send("Alert", "Should raise", Severity.ERROR)


class TestWebhookAlertResource:
    def test_mock_mode_logs_when_no_url(self, caplog: pytest.LogCaptureFixture) -> None:
        resource = WebhookAlertResource()
        with caplog.at_level(logging.INFO):
            resource.send("Test Alert", "Test content", Severity.WARNING)
        assert "[MOCK]" in caplog.text

    def test_custom_headers(self) -> None:
        resource = WebhookAlertResource(
            webhook_url="https://example.com/hook",
            headers={"Authorization": "Bearer token123"},
        )
        assert resource.headers["Authorization"] == "Bearer token123"

    @patch("urllib.request.urlopen")
    def test_real_mode_sends_request(self, mock_urlopen: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        resource = WebhookAlertResource(webhook_url="https://example.com/hook")
        resource.send("Alert", "Content", Severity.ERROR)

        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_real_mode_handles_failure_gracefully(
        self, mock_urlopen: MagicMock
    ) -> None:
        mock_urlopen.side_effect = Exception("Connection refused")

        resource = WebhookAlertResource(webhook_url="https://example.com/hook")
        with pytest.raises(Exception, match="Connection refused"):
            resource.send("Alert", "Should not crash", Severity.ERROR)
