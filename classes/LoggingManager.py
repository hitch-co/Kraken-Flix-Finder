import logging
import functools
import pprint

class LoggingManager:
    def __init__(self, dirname='log', logger_name=None, debug_level='DEBUG', mode='w', stream_logs=True, encoding='UTF-8'):
        self.dirname = dirname
        self.logger_name = logger_name
        self.debug_level = debug_level.upper()
        self.mode = mode
        self.stream_logs = stream_logs
        self.encoding = encoding

    def create_logger(self):
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL,
        }

        if self.debug_level not in level_mapping:
            raise ValueError(f"Invalid debug_level: {self.debug_level}. Must be one of: {', '.join(level_mapping.keys())}")

        logger = logging.getLogger(self.logger_name if self.logger_name else __name__)
        
        # Clear existing handlers
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        logger.setLevel(level_mapping[self.debug_level])

        # Create the formatter
        formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - Name: %(funcName)s - Line: %(lineno)d - %(message)s')

        file_handler = logging.FileHandler(f'{self.dirname}/{self.logger_name}.log', mode=self.mode, encoding=self.encoding)
        file_handler.setLevel(level_mapping[self.debug_level])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if self.stream_logs:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level_mapping[self.debug_level])
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
        
        return logger

    @classmethod
    def log_class_args(cls, logger_name):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                logger = logging.getLogger(logger_name)  
                logger.info("These are the function arguments".center(30, '-'))
                
                formatted_args = pprint.pformat(args, indent=4, width=1)  # Format positional arguments
                formatted_kwargs = pprint.pformat(kwargs, indent=4, width=1)  # Format keyword arguments
                
                logger.info("Keyword arguments: \n%s", formatted_kwargs)
                logger.info("Positional arguments: \n%s", formatted_args)
                return func(self, *args, **kwargs)
            return wrapper
        return decorator
    
def main():
    logger = LoggingManager(
        dirname='log', 
        logger_name='example', 
        debug_level='DEBUG', 
        mode='w', 
        stream_logs=True, 
        encoding='UTF-8'
        ).create_logger()
    logger.info('This is a test message')
    logger.debug('This is a debug message')
    logger.error('This is an error message')
    logger.warning('This is a warning message')