# utils/libs/mirco_services_data_management/mirco_services_data_management/logger.py

import logging

def setup_logging(log_level_str: str = "INFO") -> logging.Logger:
    """
    Настраивает логирование в stdout. Возвращает root-логгер.
    :param log_level_str: Уровень логирования ("DEBUG", "INFO", "WARNING", ...)
    """
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger()
    logger.info(f"Logging configured at level {log_level_str.upper()}")
    return logger
