import logging


logging.basicConfig(
    filename="tasks_logs.log",
    filemode="w",
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG
)
