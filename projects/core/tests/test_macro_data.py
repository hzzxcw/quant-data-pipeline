import pytest
from unittest.mock import MagicMock
from requests.exceptions import RequestException


class TestIndustrialValueAdded:
    """测试工业增加值数据获取 asset。"""

    def test_asset_is_defined(self):
        """Test that asset is properly defined."""
        from core.defs.industrial_value_added import industrial_value_added

        assert industrial_value_added is not None

    def test_api_failure_raises_exception(self, monkeypatch):
        """测试 API 请求失败时抛出异常。"""
        import dagster as dg
        from core.defs.industrial_value_added import industrial_value_added

        def mock_get(*args, **kwargs):
            raise RequestException("Connection failed")

        monkeypatch.setattr("requests.get", mock_get)

        context = dg.build_asset_context()
        with pytest.raises(RequestException, match="Connection failed"):
            industrial_value_added(context)

    def test_api_timeout_raises_exception(self, monkeypatch):
        """测试 API 请求超时时抛出异常。"""
        import dagster as dg
        from core.defs.industrial_value_added import industrial_value_added
        import requests

        def mock_get(*args, **kwargs):
            raise requests.Timeout("Request timed out")

        monkeypatch.setattr("requests.get", mock_get)

        context = dg.build_asset_context()
        with pytest.raises(requests.Timeout, match="Request timed out"):
            industrial_value_added(context)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
