#!/usr/bin/env bash

CATALOGS_DIR=$1

# Instrucciones a ser ejecutadas una vez terminado el ETL.
# La variable CATALOGS_DIR contiene el path donde se ubican
# los archivos creados: catálogos en formato data.json y
# archivos de distribuciones.

# Este archivo se ejecuta al finalizar el ETL y sirve para copiar
# los archivos producidos en su directorio final

cp -rf $CATALOGS_DIR /var/www/html/
