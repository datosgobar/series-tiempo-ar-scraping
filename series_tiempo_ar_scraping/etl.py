import click

from series_tiempo_ar_scraping.etl_class import Etl


@click.group()
def cli():
    pass


@cli.command()
def etl():
    etl_class = Etl()
    etl_class.run()

if __name__ == '__main__':
    cli()
