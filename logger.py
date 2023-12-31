import logging
import sys
from os import makedirs
from os.path import dirname, exists

loggers = {}

LOG_ENABLED = True  # 是否开启日志
LOG_TO_CONSOLE = True  # 是否输出到控制台
LOG_TO_FILE = True  # 是否输出到文件

LOG_PATH = './logs/runtime.log'  # 日志文件路径
LOG_LEVEL = 'DEBUG'  # 日志级别
LOG_FORMAT = '%(levelname)5s:%(asctime)s - process: %(process)d - %(name)s - %(filename)s:Line %(lineno)d:%(module)s >>> %(message)s'  # 每条日志输出格式
APP_ENVIRONMENT = 'dev'  # 运行环境，如测试环境还是生产环境

def get_logger(name=None):
    """
    get logger by name
    :param name: name of logger
    :return: logger
    """
    global loggers

    if not name: name = __name__

    if loggers.get(name):
        return loggers.get(name)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # 输出到控制台
    if LOG_ENABLED and LOG_TO_CONSOLE:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # 输出到文件
    if LOG_ENABLED and LOG_TO_FILE:
        # 如果路径不存在，创建日志文件文件夹
        log_dir = dirname(LOG_PATH)
        if not exists(log_dir): makedirs(log_dir)
        # 添加 FileHandler
        file_handler = logging.FileHandler(LOG_PATH, encoding='utf-8')
        file_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    loggers[name] = logger
    return logger