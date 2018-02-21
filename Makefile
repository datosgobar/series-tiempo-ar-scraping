SHELL = bash

.PHONY: all clean download_catalogs data/params/scraping_urls.txt data/params/distribution_urls.txt download_sources upload_catalog upload_datasets send_transformation_report install_anaconda clone_repo setup_environment create_dir download_sources data/params/scraping_urls.txt

# git clone https://github.com/datosgobar/series-tiempo-ar-etl.git && cd series-tiempo
# ambiente testeado para un Ubuntu 16.04
# especificar el tipo de ambiente con SERVER_ENVIRONMENT:
# `make setup SERVER_ENVIRONMENT=dev`
# `make setup SERVER_ENVIRONMENT=prod`
setup: install_anaconda setup_environment create_dir install_cron install_nginx start_nginx

# recetas para correr el ETL
all: extraction transformation
extraction: extract_catalogs send_extraction_report data/params/scraping_urls.txt data/params/distribution_urls.txt download_sources
transformation: scrape_datasets send_transformation_report

# SETUP
install_anaconda:
	wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
	bash Miniconda2-latest-Linux-x86_64.sh
	source ~/.bashrc

# para esto es necesario frenar cualquier otro servicio web en el puerto 80
# también conviene instalar el monitoreo con amplify
install_nginx:
	# sudo service apache2 stop
	sudo apt-get update && sudo apt-get install nginx && sudo apt-get install nginx-extras
	sudo ufw allow 'Nginx HTTP'
	sudo cp scripts/config/nginx.conf /etc/nginx/nginx.conf
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
	sudo cp scripts/config/nginx.conf /etc/nginx/nginx.conf
	sudo systemctl start nginx

stop_nginx:
	# Usar en casos extremos
	# killall nginx
	# sudo nginx -s stop
	sudo systemctl stop nginx

restart_nginx: stop_nginx start_nginx

# ambiente testeado para un Ubuntu 16.04
# especificar el tipo de ambiente con SERVER_ENVIRONMENT:
# `make setup_environment SERVER_ENVIRONMENT=dev`
# `make setup_environment SERVER_ENVIRONMENT=prod`
setup_environment:
	source ~/.bashrc
	conda create -n series-tiempo --no-default-packages
	echo 'export SERIES_TIEMPO_PYTHON=~/miniconda2/envs/series-tiempo/bin/python' >> ~/.bashrc
	echo 'export SERVER_ENVIRONMENT=$(SERVER_ENVIRONMENT)' >> ~/.bashrc
	source ~/.bashrc
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
	mkdir -p data/test_output
	mkdir -p data/test_output/server/catalog
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
	@echo "SERVER_ENVIRONMENT=$(SERVER_ENVIRONMENT)" >> .cronfile
	cat cron_jobs >> .cronfile
	crontab .cronfile
	rm .cronfile
	touch cron_jobs

# EXTRACTION
extract_catalogs:
	$(SERIES_TIEMPO_PYTHON) scripts/extract_catalogs.py "data/params/indice.yaml" "data/output/server/catalog"

send_extraction_report:
	$(SERIES_TIEMPO_PYTHON) scripts/send_email.py extraccion

data/params/scraping_urls.txt:
	$(SERIES_TIEMPO_PYTHON) scripts/generate_urls.py "data/output/server/catalog" "scraping" "$@"

data/params/distribution_urls.txt:
	$(SERIES_TIEMPO_PYTHON) scripts/generate_urls.py "data/output/server/catalog" "distribution" "$@"

download_sources:
	bash scripts/download_scraping_sources.sh "data/params/scraping_urls.txt"
	bash scripts/download_distributions.sh "data/params/distribution_urls.txt"

# TRANSFORMATION
# TODO: revisar como se usan adecuadamenten los directorios
scrape_datasets:
	$(SERIES_TIEMPO_PYTHON) scripts/scrape_datasets.py \
		data/output/server/catalog/{}/data.json \
		data/input/catalog/{}/sources/ \
		data/output/server/catalog/{}/dataset/ \
		{} \
		replace

send_transformation_report:
	$(SERIES_TIEMPO_PYTHON) scripts/send_email.py scraping

# CLEAN
clean:
	rm -rf data/input/
	rm -rf data/output/
	rm -rf data/test_output/
	rm -f data/params/scraping_urls.txt
	rm -f data/params/distribution_urls.txt
	rm -rf data/reports
	make create_dir

# TEST
profiling_test: data/output/server/catalog/$(PROFILING_CATALOG_ID)/data.json
	$(SERIES_TIEMPO_PYTHON) -m scripts.tests.profiling $^ \
		data/input/catalog/$(PROFILING_CATALOG_ID)/sources/ \
		data/test_output/server/catalog/$(PROFILING_CATALOG_ID)/dataset/ \
		$(PROFILING_CATALOG_ID)
