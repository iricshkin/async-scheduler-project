import multiprocessing
from datetime import datetime

import my_logger
from job import Job, download_weather, make_dir, move_files
from scheduler import Scheduler

logger = my_logger.get_logger(__name__)


start_time = datetime.now()
condition = multiprocessing.Condition()
J1 = Job(
    name='J1 - Download weather for Moscow',
    start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    func=download_weather,
    dependencies=[],
    args=('Москва', '2021', '06'),
)
J2 = Job(
    name='J2 - Download weather for St. Petersburg',
    start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    func=download_weather,
    args=('Санкт-Петербург', '2019', '08'),
)
J3 = Job(
    name='J3 - Download weather for Okulovka',
    start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    func=download_weather,
    args=('Окуловка', '2016', '07'),
)
J4 = Job(
    name='J4 - Download weather for Borovichi',
    start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    func=download_weather,
    args=('Боровичи', '2020', '08'),
)
J5 = Job(
    name='J4 - Download weather for Guangzhou',
    start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    func=download_weather,
    args=('Гуанчжоу', '2018', '09'),
)
J6 = Job(
    name='J6 - Create archive and directories',
    start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    func=make_dir,
    dependencies=[J1, J2, J3, J4, J5],
)
J7 = Job(
    name='J7 - Transfer files',
    start_at=datetime(2022, 10, 19, 00, 40, 0),
    max_working_time=0,
    tries=3,
    func=move_files,
    dependencies=[J6],
)
J8 = Job(
    name='J8 -Transfer files',
    start_at=datetime(2022, 10, 19, 00, 41, 0),
    max_working_time=0,
    tries=3,
    func=move_files,
    dependencies=[J1, J2],
)
scheduler1 = Scheduler()
manager = scheduler1.schedule()
manager.send((J7, J2, J5, J6, J1, J3, J4))
manager.send((J1, J2, J8))
manager.close()
logger.info(f'Lead time {datetime.now() - start_time} s.')
