import os
import click
import yaml

from series_tiempo_ar_scraping.base import ETL

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(ROOT_DIR, "config")


def read_config(file_path):
    try:
        with open(file_path) as config_data:
            return yaml.load(config_data)
    except:
        raise "El formato del archivo de configuración es inválido"

@click.group()
def cli():
    pass


@cli.command()
@click.option(
    '--config',
    default=lambda: os.path.join(CONFIG_DIR, 'index.yaml'),
    type=click.Path(exists=True),
)
def etl(config):
    config = read_config(file_path=config)
    etl_class = ETL(config)
    etl_class.run()

if __name__ == '__main__':
    cli()
