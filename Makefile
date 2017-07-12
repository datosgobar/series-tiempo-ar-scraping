.PHONY: all clean download_catalog download_excels update_catalog update_datasets send_transformation_report install_anaconda clone_repo setup_environment create_dir

all: extraction transformation
extraction: download_catalog catalogo/datos/catalogo-sspm.xlsx catalogo/datos/excels_urls.txt download_excels
transformation: catalogo/datos/data.json catalogo/datos/datasets/ send_transformation_report
load: update_catalog update_datasets
setup: install_anaconda clone_repo setup_environment create_dir install_cron


# setup
install_anaconda:
	wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
	bash Miniconda2-latest-Linux-x86_64.sh

clone_repo:
	git clone https://github.com/datosgobar/series-tiempo.git
	cd series-tiempo

# ambiente testeado para un Ubuntu 16.04
setup_environment:
	conda create -n series-tiempo --no-default-packages
	source activate series-tiempo
	conda install pycurl
	# sudo apt-get update && sudo apt-get install gcc && sudo apt-get install python-pycurl
	# sudo apt-get install libcurl4-gnutls-dev
	pip install -r requirements.txt

create_dir:
	mkdir -p catalogo
	mkdir -p catalogo/logs
	mkdir -p catalogo/datos
	mkdir -p catalogo/datos/ied
	mkdir -p catalogo/datos/datasets
	mkdir -p catalogo/datos/datasets_test
	mkdir -p catalogo/datos/catalogos
	mkdir -p catalogo/datos/reportes
	mkdir -p catalogo/codigo

install_cron: cron_jobs
	@echo "PATH=$(PATH)" >> .cronfile
	@echo "SERIES_TIEMPO_DIR=$$PWD" >> .cronfile
	@echo "SERIES_TIEMPO_PYTHON=$(SERIES_TIEMPO_PYTHON)" >> .cronfile
	cat cron_jobs >> .cronfile
	crontab .cronfile
	rm .cronfile
	touch cron_jobs

# extraction
download_catalog: catalogo/datos/catalogo_sspm_url.txt
	wget -N -i catalogo/datos/catalogo_sspm_url.txt --directory-prefix=catalogo/datos --no-check-certificate -O catalogo/datos/catalogo-sspm-downloaded.xlsx

catalogo/datos/catalogo-sspm.xlsx: catalogo/datos/catalogo-sspm-downloaded.xlsx
	[ -n $(`cmp "$<" "$@"`) ] && cp "$<" "$@"

catalogo/datos/excels_urls.txt: catalogo/datos/catalogo-sspm.xlsx
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/generate_excels_urls.py "$<" "$@"

download_excels:
	wget -N -i catalogo/datos/excels_urls.txt --directory-prefix=catalogo/datos/ied/

# transformation
catalogo/datos/data.json: catalogo/datos/catalogo-sspm.xlsx
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/generate_catalog.py "$<" "$@"
	# $(SERIES_TIEMPO_PYTHON) catalogo/codigo/generate_catalog.py "$<" "$@" > catalogo/datos/generate-catalog-errors.txt

# TODO: revisar como se usan adecuadamenten los directorios
catalogo/datos/datasets/: catalogo/datos/data.json catalogo/datos/etl_params.csv
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/scrape_datasets.py $^ catalogo/datos/ied/ "$@"

catalogo/datos/etl_params.csv: catalogo/datos/catalogo-sspm.xlsx
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/generate_etl_params.py "$<" "$@"

send_transformation_report:
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/send_email.py catalogo/datos/reportes/mail_subject.txt catalogo/datos/reportes/mail_message.txt

# load
update_catalog: catalogo/datos/data.json
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/update_catalog.py "$<" "config_ind.yml"

update_datasets: catalogo/datos/datasets/
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/update_datasets.py "$<" "config_ind.yml"

# clean
clean:
	rm -f catalogo/datos/catalogo-sspm-downloaded.xlsx
	rm -f catalogo/datos/catalogo-sspm.xlsx
	rm -f catalogo/datos/excels_urls.txt
	rm -rf catalogo/datos/ied/
	rm -f catalogo/datos/data.json
	rm -f catalogo/datos/etl_params.csv
	rm -rf catalogo/datos/datasets/
	make create_dir

# test
profiling_test: catalogo/datos/data.json catalogo/datos/etl_params_test.csv
	$(SERIES_TIEMPO_PYTHON) catalogo/codigo/profiling.py $^ catalogo/datos/ied/ catalogo/datos/datasets_test/



