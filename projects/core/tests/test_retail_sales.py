import pytest


class TestRetailSales:
    """Test retail sales asset."""

    def test_asset_is_defined(self):
        """Test that asset is properly defined."""
        from core.defs.retail_sales import retail_sales

        assert retail_sales is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
