import pytest
from unittest.mock import MagicMock, patch

from core.defs.macro_data import industrial_value_added, _generate_mock_data


class TestIndustrialValueAdded:
    """测试工业增加值数据获取 asset。"""

    def test_generate_mock_data(self):
        """测试模拟数据生成功能。"""
        context = MagicMock()

        result = _generate_mock_data(context)

        assert result is not None
        assert "row_count" in result.metadata
        assert result.metadata["row_count"].value == 2
        assert result.metadata["data_type"].value == "mock_data"
        assert "sample" in result.metadata

    def test_mock_data_structure(self):
        """测试模拟数据的结构。"""
        context = MagicMock()

        result = _generate_mock_data(context)
        sample = result.metadata["sample"].value

        assert len(sample) == 2
        assert "indicator" in sample[0]
        assert "period" in sample[0]
        assert "value" in sample[0]

    def test_mock_data_generation_logic(self):
        """测试模拟数据生成逻辑返回正确的结构。"""
        context = MagicMock()
        result = _generate_mock_data(context)

        metadata = result.metadata
        assert metadata["row_count"].value == 2
        assert metadata["data_type"].value == "mock_data"
        assert metadata["latest_period"].value == "202602"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
