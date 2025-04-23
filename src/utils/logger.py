import logging
import functools
import os

logging.basicConfig(level=logging.DEBUG)

LOG_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "app-logs", "app.log"
)


class Logging:
    # Initialize class variable
    logger = logging.getLogger("app-logger")

    def __init__(self):
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        # Set logger level
        self.logger.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter(
            "[%(asctime)s] {%(filename)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s",
            "%Y-%m-%d %H:%M",
        )

        # Add handlers only if they don't exist
        if not self.logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            # File handler
            file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)

            # Add handlers
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

    def log(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                self.logger.debug(
                    f"Calling: {func.__name__}, args={args}, kwargs={kwargs}"
                )
                res = func(*args, **kwargs)
                self.logger.debug(f"{func.__name__} returned: {res}")
                return res  # Added missing return statement
            except Exception as e:
                self.logger.exception(e)
                raise e

        return wrapper
