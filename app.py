from server.server import Server
from logger.console_logger import ConsoleLogger


if __name__ == "__main__":
    logger = ConsoleLogger(name="MessageQueue").get_logger()
    server = Server(logger, host='localhost', port=5000,)
    server.start()
