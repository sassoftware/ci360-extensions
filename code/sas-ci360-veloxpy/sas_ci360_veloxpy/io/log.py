import logging

# Only create named loggers, do not configure handlers
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"sas_ci_360_veloxpy.{name}")
    logger.propagate = True  # Allow client to catch logs
    return logger
