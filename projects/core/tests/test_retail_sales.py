import pytest
from unittest.mock import MagicMock

from core.defs.retail_sales import retail_sales, _generate_mock_data


class TestRetailSales:
    """测试社会消费品零售总额数据获取 asset。"""

    def test_generate_mock_data(self):
        """测试模拟数据生成功能。"""
        context = MagicMock()

        result = _generate_mock_data(context)

        assert result is not None
        assert "row_count" in result.metadata
        assert result.metadata["row_count"].value == 3
        assert result.metadata["data_type"].value == "mock_data"
        assert "sample" in result.metadata

    def test_mock_data_structure(self):
        """测试模拟数据的结构。"""
        context = MagicMock()

        result = _generate_mock_data(context)
        sample = result.metadata["sample"].value

        assert len(sample) == 3
        assert "indicator" in sample[0]
        assert "period" in sample[0]
        assert "value" in sample[0]

    def test_mock_data_content(self):
        """测试模拟数据的内容。"""
        context = MagicMock()

        result = _generate_mock_data(context)
        sample = result.metadata["sample"].value

        indicators = [s["indicator"] for s in sample]
        assert "零售总额当月同比" in indicators
        assert "餐饮收入当月同比" in indicators


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
