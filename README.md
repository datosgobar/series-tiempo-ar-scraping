# series-tiempo-ar-scraping

ETL y servidor web que scrapea series de tiempo de archivos `.xlsx` semi-estructurados y los transforma en distribuciones de formato abierto. La aplicación está basada en una extensión experimental del Perfil de Metadatos del Paquete de Apertura de Datos.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

## Indice

- [Instalación](#instalacion)
- [Uso](#uso)
  - [Correr el ETL](#correr-el-etl)
  - [Entradas/Salidas del ETL](#entradassalidas-del-etl)
- [Contacto](#contacto)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Instalación
Los siguientes pasos fueron probados en una instalación de Ubuntu 16.04.

1. Instalar las dependencias necesarias para descargar y configurar el proyecto:

```bash
$ sudo apt install python-pip git
```

2. Clonar el repositorio y acceder al directorio creado:
```bash
$ git clone https://github.com/datosgobar/series-tiempo-ar-scraping.git
$ cd series-tiempo-ar-scraping
```

3. Crear el entorno virtual de Python con *Anaconda* o *Virtualenv* e instalar las dependencias del proyecto:

    **Anaconda:**

    Si *Anaconda* **no** se encuentra instalado, instalarlo:
    ```bash
    $ make install_anaconda
    ```
    Durante el proceso de instalación, asegurar que el instalador modifique el archivo `.bashrc` para incluir el comando `conda` en el `PATH`.

    Luego, habilitar el comando `conda`:
    ```bash
    $ source ~/.bashrc
    ```
    Una vez instalado *Anaconda* y habilitado el comando `conda`, crear el entorno virtual:
    ```bash
    $ make setup_anaconda
    ```
    **Virtualenv:**

    Crear el entorno virtual e instalar las dependencias:
    ```bash
    $ make setup_virtualenv
    ```

4. Crear el índice de catálogos y el archivo de configuración general:
```bash
$ cp config/index.example.yaml config/index.yaml
$ cp config/config_general.example.yaml config/config_general.yaml
```

El archivo `index.yaml` contiene el listado de catálogos a ser descargados y scrapeados. Por defecto, incluye un catálogo de ejemplo llamado `example_catalog1`, cuyos archivos están almacenados en este repositorio.

5. **(Opcional)** Crear los archivos de configuración para el envio de reportes por mail y para descargas:

```bash
$ cp config/config_email.example.yaml config/config_email.yaml
$ cp config/config_downloads.example.yaml config/config_downloads.yaml
```

Luego, editar los archivos `config_email.yaml` y `config_downloads.yaml` con los parámetros deseados.

## Uso

### Correr el ETL

Los distintos pasos del *scraper* se pueden correr individualmente como recetas del `Makefile`. Para correr el ETL completo:

**Anaconda:**
```bash
$ source activate series-tiempo-ar-scraping
$ make all
```

**Virtualenv:**
```bash
$ source series-tiempo-ar-scraping/bin/activate
$ make all
```

El proceso toma catálogos de datos abiertos en Excel con series de tiempo documentas para *scraping*, transforma el catálogo al formato `data.json` y genera (distribuciones para *scraping*) o descarga (distribuciones ya generadas según la especificación) los archivos de distribuciones en el directorio `data/output`.

Si se corre luego de los pasos de instalación, el proceso se ejecuta con el catálogo de ejemplo.

### Entradas/Salidas del ETL

- **Entradas**:
    - `index.yaml`: Contiene un listado de catálogos con series de tiempo y sus URLs respectivas ([Ver ejemplo](config/index.example.yaml)).
        + Ver ejemplo de [catálogo en Excel](https://github.com/datosgobar/series-tiempo-ar-scraping/raw/master/samples/catalogs/example_catalog1.xlsx)
    - `config_general.yaml`: Contiene la configuración del servidor donde se servirán los archivos de salida ([Ver ejemplo](config/config_general.example.yaml)).

- **Salidas**:
    - Directorio `data/output/server/`: Por cada catálogo procesado, se crea un subdirectorio con:
        - `catalog.xlsx`: Catálogo en formato `.xlsx` (`data/output/server/catalog/{catalog_id}/catalog.xlsx`).
        - `catalog.json`: Catálogo en formato `.json` (`data/output/server/catalog/{catalog_id}/data.json`).
        - Archivos de distribuciones descargados vía `downloadURL` (`data/output/server/catalog/{catalog_id}/dataset/{dataset_id}/distribution/{distribution_id}/distribucion-descargada-nombre.csv`).
        - Archivos de distribuciones *scrapeadas* (`data/output/server/catalog/{catalog_id}/dataset/{dataset_id}/distribution/{distribution_id}/distribucion-scrapeada-nombre.csv`).
    - Directorio `data/reports/`: Por cada catálogo procesado, se crea un subdirectorio con:
        - Reporte del proceso de validación del catálogo.
        - Reporte con información sobre los *datasets* del catálogo.
        - Reportes del proceso de *scraping* del catálogo.

## Contacto

Te invitamos a [crearnos un issue](https://github.com/datosgobar/series-tiempo-ar-scraping/issues/new?title=Encontre%20un%20bug%20en series-tiempo-ar-scraping) en caso de que encuentres algún bug o tengas feedback de alguna parte de `series-tiempo-ar-scraping`.

Para todo lo demás, podés mandarnos tu comentario o consulta a [datos@modernizacion.gob.ar](mailto:datos@modernizacion.gob.ar).
