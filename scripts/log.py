import logging


def init(file_name, aws_lambda=False, debug=False):
    # Setup logging
    logging.basicConfig()
    logger = logging.getLogger()

    if debug:
        # Debug mode if -debug argument is used in execution.
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if not aws_lambda:
        # create a file handler
        handler = logging.FileHandler(file_name)
        if debug:
            # Debug mode if -debug argument is used in execution.
            handler.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)

    return logger
