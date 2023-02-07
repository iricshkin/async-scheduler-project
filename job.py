import json
import os
import shutil
import time
import warnings
import weakref
from collections import defaultdict
from datetime import date, datetime
from typing import Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from wrapt_timeout_decorator import timeout

import my_logger
from enums import Status, Wrong

logger = my_logger.get_logger(__name__)

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


CHECK_PERIOD: int = 5
HEADERS: dict = {'user-agent': 'my-app/0.0.1'}
CITIES_DICT: dict = {
    'Москва': 4368,
    'Санкт-Петербург': 4079,
    'Окуловка': 4118,
    'Боровичи': 4121,
    'Гуанчжоу': 6455,
}


def download_weather(city: str, year: str = '2022', month: str = '09') -> str:
    """Функция скачивания погоды в файл эксель."""
    time.sleep(2)
    dict_temp = {}
    try:
        city_cod = CITIES_DICT[city]
    except KeyError:
        logger.error('There is no information for the city %r!', city)
        return Wrong.CITY
    url = f'https://www.gismeteo.ru/diary/{city_cod}/{year}/{month}/'
    try:
        r = requests.get(url, headers=HEADERS, verify=False)
        soup = BeautifulSoup(r.text, 'lxml')
        temp = soup.find_all('td', {'class': 'first_in_group'})
    except requests.exceptions.ConnectionError:
        logger.exception('No connection to %r!', url)
        return Wrong.CONNECTION
    i = 0
    list_value = []
    for t in temp:
        if i % 2 == 0:
            list_value.append((t.text))
        i = i + 1
    dlina = 31 - len(list_value)
    for _ in range(dlina):
        list_value.append('300')
    dict_temp[city] = list_value
    datatemp = pd.DataFrame(
        dict_temp,
        index=[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
        ]
    )
    datatemp = datatemp.T
    datatemp.to_excel(f'Weather-{year}.{month}-{city}.xlsx')
    return Status.SUCCESS.value


def get_file_weather_list() -> tuple[str, list[str]]:
    """Функция получения текущей директории и списка файлов с погодой."""
    curr_dir = os.getcwd()
    file_list = os.listdir('.')
    weather_file_list = []
    for file in file_list:
        if 'Weather' in file:
            weather_file_list.append(file)
    return curr_dir, weather_file_list


def make_dir() -> str:
    """Функция создания архива и директорий."""
    curr_dir, weather_file_list = get_file_weather_list()
    if os.path.exists(curr_dir + '//' + 'WEATHER_ARCHIVE'):
        os.chdir(curr_dir + '//' + 'WEATHER_ARCHIVE')
    else:
        os.mkdir('WEATHER_ARCHIVE')
        os.chdir(curr_dir + '//' + 'WEATHER_ARCHIVE')
    for file in weather_file_list:
        year_dir = file[8:12]
        if os.path.exists(curr_dir + '//' + 'WEATHER_ARCHIVE' + year_dir):
            continue
        else:
            os.mkdir(year_dir)
    os.chdir(curr_dir)
    return Status.SUCCESS.value


def move_files() -> str:
    """Функция переноса файлов в архив."""
    curr_dir, weather_file_list = get_file_weather_list()
    for file in weather_file_list:
        year_dir = file[8:12]
        if os.path.exists(
            curr_dir + '//' + 'WEATHER_ARCHIVE' + year_dir
            + '//' + file
        ):
            os.remove(file)
        else:
            shutil.move(
                curr_dir + '//' + file,
                curr_dir + '//' + 'WEATHER_ARCHIVE' + year_dir
            )
    return Status.SUCCESS.value


def dep_done(dep_list: list) -> bool:
    if dep_list:
        try:
            with open('data.txt') as json_file:
                task_dict = json.load(json_file)
        except FileNotFoundError as exp:
            logger.exception('An exception is thrown %r', exp)
        for dep in dep_list:
            if task_dict[dep.name] == Status.RUN.value:
                return False
    return True


class SaveRefs:
    __refs__ = defaultdict(list)

    def __init__(self) -> None:
        self.__refs__[self.__class__].append(weakref.ref(self))

    @classmethod
    def get_instances(cls) -> object:
        for inst_ref in cls.__refs__[cls]:
            inst = inst_ref()
            if inst is not None:
                yield inst


class Job(SaveRefs):
    def __init__(
        self,
        start_at: date = '',
        max_working_time: int = -1,
        tries: int = 1,
        dependencies: list = [],
        name: str = 'Job',
        func: Callable = None,
        args: list = [],
    ) -> None:
        super().__init__()
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.name = name
        self.status = Status.RUN.value
        self.func = func
        self.args = args

    def run(self) -> None:
        """Функция запуска и проверки целевых функций."""
        for tri in range(self.tries):
            while True:
                now = datetime.now()
                if (
                    (not self.start_at or self.start_at < now)
                    and dep_done(self.dependencies)
                ):
                    logger.info(
                        'Running task: [%r]. Attempt %s of %d.',
                        self.name, tri + 1, self.tries
                    )
                    try:
                        if self.max_working_time > 0:
                            f = timeout(self.max_working_time)(self.func)
                        else:
                            f = self.func
                        result_func = f(*self.args)
                    except TimeoutError:
                        logger.exception(
                            'The maximum running time of the function has '
                            'been exceeded!'
                        )
                        result_func = Wrong.TIMEOUT
                    break
                else:
                    logger.info(
                        'Waiting for start time for task [%r].', self.name
                    )
                    time.sleep(CHECK_PERIOD)
            if result_func == Status.SUCCESS.value:
                self.status = Status.DONE.value
                logger.info('Task [%r] completed.', self.name)
                try:
                    with open('data.txt') as json_file:
                        task_dict = json.load(json_file)
                except FileNotFoundError as exp:
                    logger.exception('An exception is thrown %r', exp)
                task_dict[self.name] = Status.DONE.value
                try:
                    with open('data.txt', 'w') as outfile:
                        json.dump(task_dict, outfile)
                except FileNotFoundError as exp:
                    logger.exception('An exception is thrown %r', exp)
                break
