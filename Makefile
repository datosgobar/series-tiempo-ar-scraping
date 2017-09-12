SHELL = bash

.PHONY: all clean download_catalog data/params/scraping_urls.txt data/params/distribution_urls.txt download_sources upload_catalog upload_datasets send_transformation_report install_anaconda clone_repo setup_environment create_dir download_sources data/params/scraping_urls.txt data/output/dump/

all: extraction transformation load
et: extraction transformation
extraction: download_catalog data/params/scraping_urls.txt data/params/distribution_urls.txt download_sources
transformation: data/output/server/catalog/sspm/data.json data/output/server/catalog/sspm/dataset/ send_transformation_report data/output/series/ data/output/dump/
# transformation: data/output/server/catalog/sspm/data.json data/output/server/catalog/sspm/dataset/ send_transformation_report
load: upload_series upload_dumps
setup: install_anaconda setup_environment create_dir install_cron

start_python_server:
	cd data/output/server && python -m SimpleHTTPServer 8080

# setup
install_anaconda:
	wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
	bash Miniconda2-latest-Linux-x86_64.sh
	source ~/.bashrc

install_nginx:
	sudo service apache2 stop
	sudo apt-get update && sudo apt-get install nginx
	sudo ufw allow 'Nginx HTTP'
	sudo service nginx restart

start_nginx:
	sudo nginx -p . -c scripts/config/nginx.conf

stop_nginx:
	sudo nginx -s stop

# clone_repo:
# 	git clone https://github.com/datosgobar/series-tiempo.git
# 	cd series-tiempo

# ambiente testeado para un Ubuntu 16.04
setup_environment:
	conda create -n series-tiempo --no-default-packages
	conda install -n series-tiempo pycurl
	sudo apt-get update && sudo apt-get install gcc
	# sudo apt-get install python-pycurl
	# sudo apt-get install libcurl4-gnutls-dev
	# TODO: FALTA VER COMO LOGRAR QUE SE SETEE ESTA VARIABLE DE ENTORNO
	# HOY HAY QUE AGREGARLA MANUALMENTE AL .BASHRC PARA QUE FUNCIONE
	# PREVIA BUSQUEDA EN `conda env list` a ver dónde quedó
	`dirname $(SERIES_TIEMPO_PYTHON)`/pip install -r requirements.txt

update_environment: create_dir
	git pull
	`dirname $(SERIES_TIEMPO_PYTHON)`/pip install -r requirements.txt --upgrade

create_dir:
	mkdir -p logs
	mkdir -p docs
	mkdir -p scripts
	mkdir -p data
	mkdir -p data/input
	mkdir -p data/input/catalog
	mkdir -p data/output
	mkdir -p data/output/server/catalog
	mkdir -p data/output/series
	mkdir -p data/output/dump
	mkdir -p data/params
	mkdir -p data/reports
	mkdir -p data/backup
	mkdir -p data/backup/catalog

install_cron: cron_jobs
	@echo "LANG=en_US.UTF-8" >> .cronfile
	@echo "LANGUAGE=en" >> .cronfile
	@echo "LC_CTYPE=en_US.UTF-8" >> .cronfile
	@echo "PYTHONIOENCODING=utf8" >> .cronfile
	@echo "PATH=$(PATH)" >> .cronfile
	@echo "SERIES_TIEMPO_DIR=$$PWD" >> .cronfile
	@echo "SERIES_TIEMPO_PYTHON=$(SERIES_TIEMPO_PYTHON)" >> .cronfile
	cat cron_jobs >> .cronfile
	crontab .cronfile
	rm .cronfile
	touch cron_jobs

# extraction
download_catalog: data/params/catalog_url.txt
	# descarga cada catalogo en una carpeta propia
	while read catalog_id url; do \
		mkdir -p "data/input/catalog/$$catalog_id/" ; \
		wget -N -O "data/input/catalog/$$catalog_id/catalog.xlsx" "$$url" --no-check-certificate ; \
	done < "$<"

data/params/scraping_urls.txt:
	$(SERIES_TIEMPO_PYTHON) scripts/generate_urls.py data/input/catalog/ scraping "$@"

data/params/distribution_urls.txt:
	$(SERIES_TIEMPO_PYTHON) scripts/generate_urls.py data/input/catalog/ distribution "$@"

download_sources:
	bash scripts/download_scraping_sources.sh "data/params/scraping_urls.txt"
	bash scripts/download_distributions.sh "data/params/distribution_urls.txt"

# transformation
data/output/server/catalog/sspm/data.json: data/input/catalog/sspm/catalog.xlsx
	$(SERIES_TIEMPO_PYTHON) scripts/generate_catalog.py "$<" "$@"
	# $(SERIES_TIEMPO_PYTHON) scripts/generate_catalog.py "$<" "$@" > data/generate-catalog-errors.txt

# TODO: revisar como se usan adecuadamenten los directorios
data/output/server/catalog/sspm/dataset/: data/output/server/catalog/sspm/data.json data/params/scraping_params.csv data/input/catalog/sspm/sources/
	$(SERIES_TIEMPO_PYTHON) scripts/scrape_datasets.py $^ "$@" skip
	# $(SERIES_TIEMPO_PYTHON) scripts/validate_distributions.py data/output/server/catalog/sspm/data.json data/input/catalog/sspm/dataset/ "$@" replace

data/params/scraping_params.csv: data/input/catalog/sspm/catalog.xlsx
	$(SERIES_TIEMPO_PYTHON) scripts/generate_scraping_params.py "$<" "$@"

send_transformation_report:
	$(SERIES_TIEMPO_PYTHON) scripts/send_email.py data/reports/mail_subject.txt data/reports/mail_message.txt

data/output/series/: data/output/server/catalog/sspm/data.json data/params/series_params.json
	$(SERIES_TIEMPO_PYTHON) scripts/generate_series.py $^ data/output/server "$@"

data/output/dump/:
	$(SERIES_TIEMPO_PYTHON) scripts/generate_dumps.py data/output/server "$@"

# load
upload_series:
	$(SERIES_TIEMPO_PYTHON) scripts/webdav.py data/output/series/ series "scripts/config/config_webdav.yaml" "data/params/webdav_series.json"

upload_dumps:
	$(SERIES_TIEMPO_PYTHON) scripts/webdav.py data/output/dump/ dumps "scripts/config/config_webdav.yaml" "data/params/webdav_dumps.json"

upload_catalog: data/output/server/catalog/sspm/data.json
	$(SERIES_TIEMPO_PYTHON) scripts/upload_catalog.py "$<" "scripts/config/config_ind.yaml"

upload_datasets: data/output/server/catalog/sspm/dataset/
	$(SERIES_TIEMPO_PYTHON) scripts/upload_datasets.py "$<" "scripts/config/config_ind.yaml" "scripts/config/config_webdav.yaml"

# clean
clean:
	rm -rf data/input/
	rm -rf data/output/
	rm -f data/params/scraping_urls.txt
	rm -f data/params/distribution_urls.txt
	rm -f data/params/scraping_params.csv
	make create_dir

# test
profiling_test: data/output/server/catalog/sspm/data.json data/scraping_params_test.csv
	$(SERIES_TIEMPO_PYTHON) scripts/profiling.py $^ data/input/catalog/sspm/sources/ data/datasets_test/

test_crontab:
	echo $(SERIES_TIEMPO_PYTHON)


