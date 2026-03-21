import pytest


class TestIndustrialValueAdded:
    """Test industrial value added asset."""

    def test_asset_is_defined(self):
        """Test that asset is properly defined."""
        from core.defs.industrial_value_added import industrial_value_added

        assert industrial_value_added is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
