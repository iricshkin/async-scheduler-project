from enum import Enum


class Status(Enum):
    """Represents the status of a task."""

    SUCCESS = 'success'
    RUN = 'to_run'
    DONE = 'done'


class Wrong(Enum):

    CITY = 'wrong city name'
    CONNECTION = 'wrong connection'
    TIMEOUT = 'timeout error'
