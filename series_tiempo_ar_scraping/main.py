import logging
import os
import click
import yaml

from series_tiempo_ar_scraping.base import ETL

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(ROOT_DIR, "config")


def read_config(file_path):
    try:
        with open(file_path) as config_data:
            return yaml.load(config_data, Loader=yaml.FullLoader)
    except:
        raise "El formato del archivo de configuración es inválido"


def get_logger(log_level):
    new_logger = logging.getLogger()

    new_logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    logging_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        '%Y-%m-%d %H:%M:%S')
    ch.setFormatter(logging_formatter)
    new_logger.addHandler(ch)

    return new_logger


@click.command()
@click.option(
    '--config',
    default=lambda: os.path.join(CONFIG_DIR, 'index.yaml'),
    type=click.Path(exists=True),
)
@click.option(
    '--log-level',
    default=lambda: read_config(os.path.join(CONFIG_DIR, 'config_general.yaml'))['logging'],
    type=str,
)
@click.option(
    '--replace',
    default=True,
    type=bool,
)
def cli(config, log_level, replace):
    main(config, log_level.upper(), replace)


def main(config, log_level, replace):
    config = read_config(file_path=config)
    get_logger(log_level)

    etl = ETL(
        identifier=None,
        parent=None,
        context=None,
        url=None,
        extension=None,
        config=config,
        replace=replace
    )

    etl.run()
