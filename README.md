# series-tiempo-ar-scraping

Aplicación escrita en Python 3 que scrapea series de tiempo de archivos `.xlsx` semi-estructurados y los transforma en distribuciones de formato abierto. La aplicación está basada en una extensión experimental del Perfil de Metadatos del Paquete de Apertura de Datos.


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

    $ pyenv install 3.7

### Instalación de series-tiempo-ar-scraping

    $ git clone https://github.com/datosgobar/series-tiempo-ar-scraping.git
    $ cd series-tiempo-ar-scraping
    $ pip install -e .


## Uso
### Básico
* etl (Copiar archivo de configuración)
* etl --config config/path.yaml (Con archivo de configuración personalizado)
