import logging
import functools

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

LOG_FILE = "app-logs/app.log"


class Logging:
    # Initialize class variable
    logger = logging.getLogger("app-logger")

    def __init__(self):
        # Set logger level
        self.logger.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(message)s",
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
            filename = func.__code__.co_filename
            try:
                self.logger.info(
                    f"Calling: {func.__name__} in {filename}, args={args}, kwargs={kwargs}"
                )
                res = func(*args, **kwargs)
                self.logger.info(f"{func.__name__} succeed")
                return res  # Added missing return statement
            except Exception as e:
                self.logger.exception(e)
                raise e

        return wrapper
