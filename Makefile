PYTHON=/Users/abenassi/anaconda/envs/series-tiempo/bin/python2.7

.PHONY: all clean download_excels update_catalog update_datasets catalogo/datos/datasets

all: extraction transformation load
extraction: catalogo/datos/excels_urls.txt download_excels
transformation: catalogo/datos/data.json catalogo/datos/datasets
load: update_catalog update_datasets
setup: create_dir


# setup
create_dir:
	mkdir -p catalogo
	mkdir -p catalogo/logs
	mkdir -p catalogo/datos
	mkdir -p catalogo/datos/ied
	mkdir -p catalogo/datos/datasets
	mkdir -p catalogo/datos/catalogos
	mkdir -p catalogo/datos/reportes
	mkdir -p catalogo/codigo

# extraction
catalogo/datos/excels_urls.txt: catalogo/datos/catalogo-sspm.xlsx
	$(PYTHON) catalogo/codigo/generate_excels_urls.py "$<" "$@"

download_excels:
	wget -N -i catalogo/datos/excels_urls.txt --directory-prefix=catalogo/datos/ied/

# transformation
catalogo/datos/data.json: catalogo/datos/catalogo-sspm.xlsx
	$(PYTHON) catalogo/codigo/generate_catalog.py "$<" "$@"
# TODO: revisar como se usan adecuadamenten los directorios
catalogo/datos/datasets: catalogo/datos/data.json catalogo/datos/etl_params.csv
	$(PYTHON) catalogo/codigo/scrape_datasets.py $^ catalogo/datos/ied/ "$@"

catalogo/datos/etl_params.csv: catalogo/datos/catalogo-sspm.xlsx
	$(PYTHON) catalogo/codigo/generate_etl_params.py "$<" "$@"

# load
update_catalog: catalogo/datos/data.json
	$(PYTHON) catalogo/codigo/update_catalog.py "$<" "config_ind.yml"

update_datasets: catalogo/datos/datasets/
	$(PYTHON) catalogo/codigo/update_datasets.py "$<" "config_ind.yml"

# clean
clean:
	rm -f catalogo/datos/excels_urls.txt
	rm -f catalogo/datos/etl_params.csv
	rm -f catalogo/datos/data.json
	rm -rf catalogo/datos/ied/
	rm -rf catalogo/datos/datasets/
