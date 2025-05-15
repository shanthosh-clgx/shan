import pytest
import pandas as pd
from datetime import datetime
from src.github_copilot.create_etl_pipeline import transform_stock_data


def test_transform_stock_data_basic_calculations():
    # Prepare sample input data
    data = {
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "open": [100.0, 105.0, 110.0],
        "close": [105.0, 110.0, 108.0],
        "high": [106.0, 112.0, 115.0],
        "low": [99.0, 104.0, 107.0],
        "volume": [1000, 1200, 900],
        "symbol": ["MSFT", "MSFT", "MSFT"],
    }
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    # Call the function to test
    result = transform_stock_data(df)

    # Assertions
    assert len(result) == 3

    # Check daily change calculation
    assert result.iloc[0]["daily_change"] == 5.0  # 105 - 100
    assert result.iloc[1]["daily_change"] == 5.0  # 110 - 105
    assert result.iloc[2]["daily_change"] == -2.0  # 108 - 110

    # Check daily change percentage calculation
    assert round(result.iloc[0]["daily_change_pct"], 2) == 5.0  # (5 / 100) * 100
    assert round(result.iloc[1]["daily_change_pct"], 2) == 4.76  # (5 / 105) * 100
    assert round(result.iloc[2]["daily_change_pct"], 2) == -1.82  # (-2 / 110) * 100

    # Check previous close calculation
    assert result.iloc[0]["prev_close"] == 100.0  # First row, prev_close is set to open
    assert result.iloc[1]["prev_close"] == 105.0
    assert result.iloc[2]["prev_close"] == 110.0

    # Check day-over-day percentage calculation
    assert result.iloc[0]["day_over_day_pct"] == 0.0  # First row is set to 0
    assert (
        round(result.iloc[1]["day_over_day_pct"], 2) == 4.76
    )  # ((110 - 105) / 105) * 100
    assert (
        round(result.iloc[2]["day_over_day_pct"], 2) == -1.82
    )  # ((108 - 110) / 110) * 100

    # Check that etl_timestamp is added
    assert "etl_timestamp" in result.columns
    assert all(isinstance(dt, datetime) for dt in result["etl_timestamp"])


def test_transform_stock_data_empty_dataframe():
    # Test with empty dataframe
    empty_df = pd.DataFrame(
        columns=["date", "open", "close", "high", "low", "volume", "symbol"]
    )
    result = transform_stock_data(empty_df)

    # Should return empty dataframe with all the expected columns
    assert len(result) == 0
    expected_columns = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "symbol",
        "daily_change",
        "daily_change_pct",
        "prev_close",
        "day_over_day_pct",
        "etl_timestamp",
    ]
    for col in expected_columns:
        assert col in result.columns


def test_transform_stock_data_single_row():
    # Test with single row dataframe
    data = {
        "date": ["2023-01-01"],
        "open": [100.0],
        "close": [105.0],
        "high": [106.0],
        "low": [99.0],
        "volume": [1000],
        "symbol": ["MSFT"],
    }
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    result = transform_stock_data(df)

    # Check calculations for single row
    assert len(result) == 1
    assert result.iloc[0]["daily_change"] == 5.0
    assert round(result.iloc[0]["daily_change_pct"], 2) == 5.0
    assert result.iloc[0]["prev_close"] == 100.0  # Should be set to open
    assert result.iloc[0]["day_over_day_pct"] == 0.0  # Should be set to 0
