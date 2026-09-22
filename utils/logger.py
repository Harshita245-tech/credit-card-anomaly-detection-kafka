import logging


def setup_logger(name, log_file):

    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )

    # File logging
    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)

    # Console logging
    console_handler = logging.StreamHandler()

    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger