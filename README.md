_ETL


Descripción corta del proyecto.


* Versión python: 3.7
* Licencia: MIT license


## Instalación

Si tiene instalado una versión anterior a Python 3.6, es posible usar pyenv para instalar Python 3.6 o superior.

### pyenv en macOS

    $ brew install readline xz

    $ brew update
    $ brew install pyenv

### pyenv en linux
Usar https://github.com/pyenv/pyenv-installer

    $ sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
      libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
      xz-utils tk-dev libffi-dev liblzma-dev python-openssl git

    $ curl https://pyenv.run | bash

### Usando pyenv

    $ pyenv install 3.6.6

### Instalación de bcra-scraper

    $ git clone https://github.com/datosgobar/series-tiempo-ar-bcra-scraping.git
    $ cd series-tiempo-ar-bcra-scraping
    $ pip install -e .

### Dependencias

* Para ejecutar el scraper es necesario tener chromedriver en el PATH, de manera que el script pueda ejecutarlo.

    brew cask install chromedriver

## Uso
### Básico
* python3 series_tiempo_ar_scraping/etl.py etl (Copiar archivo de configuración)
