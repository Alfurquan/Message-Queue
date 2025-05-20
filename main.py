from logger.console_logger import ConsoleLogger


def main():
    """
    Entry point for the application.
    """
    logger = ConsoleLogger(name="MyApp").get_logger()
    logger.info("Hello from message queue!")
    

if __name__ == "__main__":
    main()

