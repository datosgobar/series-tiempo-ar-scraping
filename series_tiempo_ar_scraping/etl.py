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

@click.group()
def cli():
    pass


@cli.command()
@click.option(
    '--config',
    default=lambda: os.path.join(CONFIG_DIR, 'index.example.yaml'),
    type=click.Path(exists=True),
)
@click.option(
    '--log_level',
    default=lambda: read_config(os.path.join(CONFIG_DIR, 'config_general.yaml'))['logging'],
    type=str,
)
def etl(config, log_level):
    config = read_config(file_path=config)
    logging.basicConfig(level=log_level)
    etl_class = ETL(
                identifier=None,
                parent=None,
                context=None,
                url=None,
                extension=None,
                config=config
                )
    etl_class.run()

if __name__ == '__main__':
    cli()
