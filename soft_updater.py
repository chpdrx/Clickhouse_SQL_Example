import sys
import queries
import os

MODULES_PATH = os.environ["MODULES"]
sys.path.append(MODULES_PATH)
from utilities.utils import get_logger, read_config
from reusable_code.clickhouse_queries import run_clickhouse_queries

if __name__ == '__main__':
    # создаем логгер
    logger = get_logger(logger_name=__name__, file_name="soft_updater")
    logger.debug(f"Starting soft_updater script...")

    # забираем креды из файла по умолчанию
    creds = read_config()["databases"]["clickhouse"]["soft"]

    run_clickhouse_queries(creds=creds, queries=queries, logger=logger, query_count=10)
