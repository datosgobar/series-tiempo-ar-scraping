.PHONY: all clean download_catalog download_sources update_catalog update_datasets send_transformation_report install_anaconda clone_repo setup_environment create_dir

all: extraction transformation load
et: extraction transformation
extraction: download_catalog data/input/catalog/sspm/catalog.xlsx data/params/sources_urls.txt download_sources
transformation: data/output/catalog/sspm/data.json data/output/catalog/sspm/dataset/ send_transformation_report data/output/series/ data/output/dumps/
# transformation: data/output/catalog/sspm/data.json data/output/catalog/sspm/dataset/ send_transformation_report
load: update_series update_dumps
setup: install_anaconda clone_repo setup_environment create_dir install_cron

start_python_server:
	cd data/output && python -m SimpleHTTPServer 8080

# setup
install_anaconda:
	wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
	bash Miniconda2-latest-Linux-x86_64.sh

install_nginx:
	sudo service apache2 stop
	sudo apt-get update && sudo apt-get install nginx
	sudo ufw allow 'Nginx HTTP'
	sudo service nginx restart

start_nginx:
	sudo service nginx restart
	sudo nginx -p . -c scripts/config/nginx.conf

stop_nginx:
	sudo systemctl stop nginx

clone_repo:
	git clone https://github.com/datosgobar/series-tiempo.git
	cd series-tiempo

# ambiente testeado para un Ubuntu 16.04
setup_environment:
	conda create -n series-tiempo --no-default-packages
	conda install -n series-tiempo pycurl
	# sudo apt-get update && sudo apt-get install gcc && sudo apt-get install python-pycurl
	# sudo apt-get install libcurl4-gnutls-dev
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
	mkdir -p data/output/catalog
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
		mkdir -p data/input/catalog/$catalog_id/ ; \
		wget -N -O "data/input/catalog/$$catalog_id/catalog.xlsx" "$$url" --no-check-certificate ; \
	done < "$<"

data/params/sources_urls.txt: data/input/catalog/sspm/catalog.xlsx
	$(SERIES_TIEMPO_PYTHON) scripts/generate_sources_urls.py "$<" "$@"

download_sources:
	wget -N -i data/params/sources_urls.txt --directory-prefix=data/input/catalog/sspm/sources

# transformation
data/output/catalog/sspm/data.json: data/input/catalog/sspm/catalog.xlsx
	$(SERIES_TIEMPO_PYTHON) scripts/generate_catalog.py "$<" "$@"
	# $(SERIES_TIEMPO_PYTHON) scripts/generate_catalog.py "$<" "$@" > data/generate-catalog-errors.txt

# TODO: revisar como se usan adecuadamenten los directorios
data/output/catalog/sspm/dataset/: data/output/catalog/sspm/data.json data/params/etl_params.csv
	$(SERIES_TIEMPO_PYTHON) scripts/scrape_datasets.py $^ data/input/catalog/sspm/sources/ "$@" replace
	# $(SERIES_TIEMPO_PYTHON) scripts/scrape_datasets.py $^ data/input/catalog/sspm/sources/ "$@" skip

data/params/etl_params.csv: data/input/catalog/sspm/catalog.xlsx
	$(SERIES_TIEMPO_PYTHON) scripts/generate_etl_params.py "$<" "$@"

send_transformation_report:
	$(SERIES_TIEMPO_PYTHON) scripts/send_email.py data/reportes/mail_subject.txt data/reportes/mail_message.txt

data/output/series/: data/output/catalog/sspm/data.json data/params/series_params.json
	$(SERIES_TIEMPO_PYTHON) scripts/generate_series.py $^ data/output/catalog/sspm/dataset/ "$@"

data/output/dump/: data/output/catalog/sspm/data.json data/params/dumps_params.json
	$(SERIES_TIEMPO_PYTHON) scripts/generate_dumps.py $^ data/output/catalog/sspm/dataset/ "$@"

# load
update_series: data/output/series/
	$(SERIES_TIEMPO_PYTHON) scripts/update_series.py "$<" "scripts/config/config_webdav.yaml" "data/params/series_params.json"

update_dumps: data/output/dumps/
	$(SERIES_TIEMPO_PYTHON) scripts/update_dumps.py "$<" "scripts/config/config_ind.yaml" "scripts/config/config_webdav.yaml"

update_catalog: data/output/catalog/sspm/data.json
	$(SERIES_TIEMPO_PYTHON) scripts/update_catalog.py "$<" "scripts/config/config_ind.yaml"

update_datasets: data/output/catalog/sspm/dataset/
	$(SERIES_TIEMPO_PYTHON) scripts/update_datasets.py "$<" "scripts/config/config_ind.yaml" "scripts/config/config_webdav.yaml"

# clean
clean:
	rm -f data/input/catalog/sspm/catalog-downloaded.xlsx
	rm -f data/input/catalog/sspm/catalog.xlsx
	rm -f data/params/sources_urls.txt
	rm -rf data/input/catalog/sspm/sources/
	rm -f data/output/catalog/sspm/data.json
	rm -f data/params/etl_params.csv
	rm -rf data/output/catalog/sspm/dataset/
	make create_dir

# test
profiling_test: data/output/catalog/sspm/data.json data/etl_params_test.csv
	$(SERIES_TIEMPO_PYTHON) scripts/profiling.py $^ data/input/catalog/sspm/sources/ data/datasets_test/

test_crontab:
	echo $(SERIES_TIEMPO_PYTHON)


