# Makefile para Ubuntu 16.04
SHELL = bash
SERIES_TIEMPO_PIP = pip2
SERIES_TIEMPO_PYTHON = python2
VIRTUALENV = bin/activate

.PHONY: all clean download_catalogs data/input/scraping_urls.txt data/input/distribution_urls.txt download_sources upload_catalog upload_datasets send_transformation_report install_anaconda clone_repo setup_environment create_dir download_sources

setup: setup_environment create_dir install_cron install_nginx start_nginx

# recetas para correr el ETL
all: extraction transformation
extraction: extract_catalogs send_extraction_report data/input/scraping_urls.txt data/input/distribution_urls.txt download_sources
transformation: scrape_datasets send_transformation_report

# para esto es necesario frenar cualquier otro servicio web en el puerto 80
# también conviene instalar el monitoreo con amplify
install_nginx:
	# sudo service apache2 stop
	sudo apt-get update && sudo apt-get install nginx && sudo apt-get install nginx-extras
	sudo ufw allow 'Nginx HTTP'
	sudo cp config/nginx.conf /etc/nginx/nginx.conf
	# sudo service nginx restart

test_nginx_conf:
	# TODO: hacer que no levante nginx si falla el test
	# sudo /etc/init.d/nginx configtest -c scripts/config/nginx.conf
	sudo /etc/init.d/nginx configtest

# FILE SERVER
start_python_server:
	cd data/output/server && python -m SimpleHTTPServer 8080

start_nginx:
	# sudo nginx -p . -c scripts/config/nginx.conf
	# /etc/init.d/nginx configtest
	sudo cp config/nginx.conf /etc/nginx/nginx.conf
	sudo systemctl start nginx

stop_nginx:
	# Usar en casos extremos
	# killall nginx
	# sudo nginx -s stop
	sudo systemctl stop nginx

restart_nginx: stop_nginx start_nginx

setup_environment:
	$(SERIES_TIEMPO_PIP) install virtualenv --upgrade
	test -d $(VIRTUALENV) || virtualenv .
	source $(VIRTUALENV)
	$(SERIES_TIEMPO_PIP) install -r requirements.txt

update_environment: create_dir
	git pull
	source $(VIRTUALENV)
	$(SERIES_TIEMPO_PIP) install -r requirements.txt --upgrade

create_dir:
	mkdir -p logs
	mkdir -p docs
	mkdir -p scripts
	mkdir -p data
	mkdir -p data/input
	mkdir -p data/input/catalog
	mkdir -p data/output
	mkdir -p data/output/server/catalog
	mkdir -p data/test_output
	mkdir -p data/test_output/server/catalog
	mkdir -p data/reports
	mkdir -p data/backup
	mkdir -p data/backup/catalog

install_cron: config/cron_jobs
	@echo "LANG=en_US.UTF-8" >> .cronfile
	@echo "LANGUAGE=en" >> .cronfile
	@echo "LC_CTYPE=en_US.UTF-8" >> .cronfile
	@echo "PYTHONIOENCODING=utf8" >> .cronfile
	@echo "PATH=$(PATH)" >> .cronfile
	@echo "SERIES_TIEMPO_DIR=$$PWD" >> .cronfile
	@echo "SERIES_TIEMPO_PYTHON=$(SERIES_TIEMPO_PYTHON)" >> .cronfile
	cat config/cron_jobs >> .cronfile
	crontab .cronfile
	rm .cronfile
	touch config/cron_jobs

# EXTRACTION
extract_catalogs:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/extract_catalogs.py "config/index.yaml" "data/output/server/catalog"

send_extraction_report:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/send_email.py extraccion

data/input/scraping_urls.txt:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/generate_urls.py "data/output/server/catalog" "scraping" "$@"

data/input/distribution_urls.txt:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/generate_urls.py "data/output/server/catalog" "distribution" "$@"

download_sources:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/download_urls.py "scraping" "data/input/scraping_urls.txt"
	$(SERIES_TIEMPO_PYTHON) scripts/download_urls.py "distribution" "data/input/distribution_urls.txt"

# TRANSFORMATION
# TODO: revisar como se usan adecuadamenten los directorios
scrape_datasets:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/scrape_datasets.py \
		data/output/server/catalog/{}/data.json \
		data/input/catalog/{}/sources/ \
		data/output/server/catalog/{}/dataset/ \
		{} \
		replace

send_transformation_report:
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) scripts/send_email.py scraping

# CLEAN
clean:
	rm -rf data/input/
	rm -rf data/output/
	rm -rf data/test_output/
	rm -rf data/reports
	make create_dir

# TEST
profiling_test: data/output/server/catalog/$(PROFILING_CATALOG_ID)/data.json
	@source $(VIRTUALENV)
	$(SERIES_TIEMPO_PYTHON) -m scripts.tests.profiling $^ \
		data/input/catalog/$(PROFILING_CATALOG_ID)/sources/ \
		data/test_output/server/catalog/$(PROFILING_CATALOG_ID)/dataset/ \
		$(PROFILING_CATALOG_ID)
