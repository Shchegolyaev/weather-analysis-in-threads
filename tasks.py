import concurrent.futures
import json
import multiprocessing
from datetime import datetime

from api_client import YandexWeatherAPI
from utils import CITIES
from logger import logging
from pydantic import BaseModel


class ResponseDataModel(BaseModel):
    forecasts: list


class ForecastsModel(BaseModel):
    date: str
    hours: list


class DataFetchingTask:
    """
    Получение данных о погоде с помощью
    APU Я.Погоды.
    """

    def get_data(self) -> dict:
        logging.info("Получение данных с API 'Я.Погоды'.")
        data = {}
        yw_api = YandexWeatherAPI()
        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                futures = {}
                for city_name in CITIES.keys():
                    futures[f"{city_name}"] = pool.submit(
                        yw_api.get_forecasting, city_name
                    )
                    logging.info(f"Город {city_name} добавлен в пул задач.")
                for city_name, future in futures.items():
                    data[f"{city_name}"] = future.result()
                    logging.info(f"Получены данные для города {city_name}.")
        except (RuntimeError, KeyError, ImportError) as error:
            logging.error(f"Ошибка в процессе получения информации: {error}.")
        return data


class DataCalculationTask:
    """
    Расчет средней температуры и колличество часов без осадков для каждого дня.
    """

    def calc_info_data_city(self, data_tuple: tuple) -> dict:
        first_day = True
        city_name, data_for_city = data_tuple
        logging.info(f"Начало расчета для города {city_name}.")
        calc_info = dict.fromkeys(
            [
                city_name,
            ]
        )
        response_data_model = ResponseDataModel(**data_for_city)
        for day in response_data_model.forecasts:
            sum_temp = 0
            hour_without_rain = 0
            count_hour_calc = 0
            forecast_model = ForecastsModel(**day)
            for hour in forecast_model.hours:
                if 9 <= int(hour["hour"]) < 19:
                    count_hour_calc += 1
                    sum_temp += int(hour["temp"])
                    if hour["condition"] in (
                        "clear",
                        "partly-cloudy",
                        "cloudy",
                        "overcast",
                    ):
                        hour_without_rain += 1

            date_input = datetime.strptime(forecast_model.date, "%Y-%m-%d")
            if count_hour_calc == 0:
                count_hour_calc += 1

            if first_day:
                calc_info[city_name] = {
                    date_input.strftime("%d-%m"): {
                        "mid_temp": round(sum_temp / count_hour_calc, 1),
                        "hour_without_rain": hour_without_rain,
                    }
                }
                first_day = False
            else:
                calc_info[city_name][date_input.strftime("%d-%m")] = {
                    "mid_temp": round(sum_temp / count_hour_calc, 1),
                    "hour_without_rain": hour_without_rain,
                }
            logging.info(f"Средние показатели города {city_name} получены.")
        return calc_info


class DataAggregationTask:
    """
    Сохранение полученных данных в файл json.
    """

    def save_to_json(self, info: list) -> None:
        with open("data_file.json", "w") as write_file:
            json.dump(info, write_file, indent=4)
        logging.info("Выолнено создание файла.")


class DataAnalyzingTask:
    """
    Выбор фаворита(-ов) по показателям температуры и дней без осадков.
    """

    def analysis_data(self, data: list) -> list:
        city_rate = []
        logging.info("Начало процесса анализа.")
        for city_data in data:
            for city in city_data:
                logging.info(f"Подсчет параметров для города {city}.")
                all_mid_temp = 0
                all_hour_without_rain = 0
                count_days = 0
                for day in city_data[city]:
                    count_days += 1
                    all_mid_temp += city_data[city][day]["mid_temp"]
                    all_hour_without_rain +=\
                        city_data[city][day]["hour_without_rain"]
                city_rate.append(
                    [
                        city,
                        round(all_mid_temp / count_days, 1),
                        round(all_hour_without_rain / count_days, 1),
                    ]
                )
                logging.info(f"Подсчет параметров для города {city} окончен.")
        city_rate.sort(
            key=lambda row: (row[1], row[2]), reverse=True
        )  # Сортировка списка списков по двум показателям.
        logging.info("Создание очереди для поиска фаворита списка.")
        ctx = multiprocessing.get_context("spawn")
        queue = ctx.Queue()
        queue.put(self.select_top_city(city_rate))
        return queue.get()

    def select_top_city(self, city_rate: list) -> list:
        favorable_cities = []
        mid_temp = 0
        days_no_rain = 0
        logging.info("Получение самого благоприятного города(-ов) "
                     "для поездки.")
        for city in city_rate:
            if city[1] > mid_temp:
                mid_temp = city[1]
                days_no_rain = city[2]
                favorable_cities.append(city)
                continue
            if city[2] > days_no_rain:
                favorable_cities.append(city)
                continue
            if city[1] == mid_temp:
                continue
            break
        return favorable_cities


if __name__ == "__main__":
    logging.info("Старт программы.")
    data = DataFetchingTask().get_data()

    logging.info("Создание пула процессов для расчета средних показателей.")
    pool = multiprocessing.Pool()
    data_calc_task = DataCalculationTask()
    logging.info("Распеределение расчета показателей городов по процессам.")
    pool_outputs = pool.map(data_calc_task.calc_info_data_city,
                            list(data.items()))
    logging.info("Окончание расчета средних показаелей для городов.")

    logging.info("Созранение информации в json-файл.")
    data_agr_task = DataAggregationTask()
    pool.apply_async(data_agr_task.save_to_json, (pool_outputs,))
    pool.close()
    pool.join()

    logging.info(
        "Получение информации о самом благорпритном(-ым) "
        "городе(-ах) для поездки."
    )
    result = DataAnalyzingTask().analysis_data(pool_outputs)
    logging.info("Вывод резуьтата.")
    for city in result:
        print(f"Наиболее благопритяным городом является {city[0]}.")
