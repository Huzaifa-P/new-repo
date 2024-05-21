import psycopg2
import logging

def connect_to_server(secrets):

    logger = logging.getLogger('Utils')

    try:
        host = secrets['host']
        port = secrets['port']
        database = secrets['database']
        user = secrets['user']
        password = secrets['password']

        logger.info('Connected To Server')
        return psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    
    except Exception as err:
        logger.error(err)
        raise Exception