# series-tiempo-ar-scraping
ETL y servidor web que scrapea series de tiempo de archivos `.xlsx` semi-estructurados y los transforma en distribuciones de formato abierto. La aplicación está basada en una extensión experimental del Perfil de Metadatos del Paquete de Apertura de Datos.

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

6. Ejecutar el ETL:

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
Al finalizar, los catálogos transformados al formato `data.json` y los archivos de distribuciones descargadas y scrapeadas se encontrarán en el directorio `data/output`.

## Entradas/Salidas del ETL
- **Entradas**:
    - `index.yaml`: Contiene listado de catálogos y sus URLs respectivas.
    - `config_general.yaml`: Contiene configuración del servidor donde ser servirán los archivos de salida.
- **Salidas**:
    - Directorio `data/output/server/`: Por cada catálogo procesado, se crea un subdirectorio con:
        - `catalog.xlsx`: Catálogo en formato `.xlsx`.
        - `catalog.json`: Catálogo en formato `.json`.
        - Archivos de distribuciones descargados vía `downloadURL`.
        - Archivos de distribuciones *scrapeadas*.
