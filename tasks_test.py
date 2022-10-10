import os

import pytest

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)


@pytest.mark.parametrize(
    "data",
    [
        DataFetchingTask().get_data(),
    ],
)
class Test_data_fetching:
    def test_is_dict(self, data):
        assert isinstance(data, dict)


@pytest.mark.parametrize(
    "data",
    [
        (
            "PARIS",
            {
                "forecasts": [
                    {
                        "date": "2022-05-26",
                        "hours": [
                            {"hour": 5, "temp": 15, "condition": "cloudy"},
                            {"hour": 10, "temp": 22, "condition": "cloudy"},
                            {"hour": 11, "temp": 19, "condition": "rain"},
                        ],
                    }
                ]
            },
        )
    ],
)
class Test_data_calculation:
    def test_calc_data(self, data):
        city_data = DataCalculationTask().calc_info_data_city(data)
        for city_name in city_data.keys():
            assert city_name == "PARIS"
            assert city_data[city_name]["26-05"]["hour_without_rain"] == 1
            assert city_data[city_name]["26-05"]["mid_temp"] == 20.5


@pytest.mark.parametrize(
    "info",
    [
        {"test_data": "test_info"},
    ],
)
class Test_aggregation:
    def test_aggregation_data(self, info):
        DataAggregationTask().save_to_json(info)
        assert os.path.exists("data_file.json")
        os.remove("data_file.json")


@pytest.mark.parametrize(
    "data",
    [
        [
            {
                "MOSCOW": {
                    "26-05": {"mid_temp": 18.0, "hour_without_rain": 7},
                    "27-05": {"mid_temp": 14.0, "hour_without_rain": 0},
                }
            },
            {
                "PARIS": {
                    "26-05": {"mid_temp": 16.0, "hour_without_rain": 3},
                    "27-05": {"mid_temp": 16.0, "hour_without_rain": 2},
                }
            },
            {
                "LONDON": {
                    "26-05": {"mid_temp": 14.0, "hour_without_rain": 10},
                    "27-05": {"mid_temp": 16.0, "hour_without_rain": 10},
                }
            },
        ],
    ],
)
class Test_analysis:
    def test_analysis(self, data):
        res_analysis = DataAnalyzingTask().analysis_data(data)
        assert len(res_analysis) == 2
        assert res_analysis[0][0] == "MOSCOW"
        assert res_analysis[0][1] == 16.0
        assert res_analysis[0][2] == 3.5
        assert res_analysis[1][0] == "LONDON"
        assert res_analysis[1][1] == 15.0
        assert res_analysis[1][2] == 10
