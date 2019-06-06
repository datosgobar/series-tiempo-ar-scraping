import click

from series_tiempo_ar_scraping.base import ETL


@click.group()
def cli():
    pass


@cli.command()
def etl():
    etl_class = ETL()
    etl_class.run()

if __name__ == '__main__':
    cli()
