import pytest
from unittest.mock import MagicMock
from requests.exceptions import RequestException


class TestRetailSales:
    """测试社会消费品零售总额数据获取 asset。"""

    def test_asset_is_defined(self):
        """Test that asset is properly defined."""
        from core.defs.retail_sales import retail_sales

        assert retail_sales is not None

    def test_api_failure_raises_exception(self, monkeypatch):
        """测试 API 请求失败时抛出异常。"""
        import dagster as dg
        from core.defs.retail_sales import retail_sales

        def mock_get(*args, **kwargs):
            raise RequestException("Connection failed")

        monkeypatch.setattr("requests.get", mock_get)

        context = dg.build_asset_context()
        with pytest.raises(RequestException, match="Connection failed"):
            retail_sales(context)

    def test_api_timeout_raises_exception(self, monkeypatch):
        """测试 API 请求超时时抛出异常。"""
        import dagster as dg
        from core.defs.retail_sales import retail_sales
        import requests

        def mock_get(*args, **kwargs):
            raise requests.Timeout("Request timed out")

        monkeypatch.setattr("requests.get", mock_get)

        context = dg.build_asset_context()
        with pytest.raises(requests.Timeout, match="Request timed out"):
            retail_sales(context)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
