# Makefile para Ubuntu 16.04
SHELL = bash
SERIES_TIEMPO_PIP ?= pip3
SERIES_TIEMPO_PYTHON ?= python3
VIRTUALENV = series-tiempo-ar-scraping
CONDA_ENV = series-tiempo-ar-scraping

.PHONY: all \
		anaconda_all \
		clean \
		create_dir \
		anaconda_dependency_install \
		anaconda_setup_virtualenv \
		anaconda_setup_etl_on_virtualenv \
		install \
		run

# recetas para correr el ETL
all: run

anaconda_all: anaconda_run

anaconda_all_error: anaconda_run_error

anaconda_install: anaconda_dependency_install anaconda_setup_virtualenv anaconda_setup_etl_on_virtualenv

anaconda_dependency_install:
	wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
	bash Miniconda3-latest-Linux-x86_64.sh
	rm Miniconda3-latest-Linux-x86_64.sh

anaconda_setup_virtualenv: create_dir
	test -d $(VIRTUALENV)/bin/activate || $(SERIES_TIEMPO_PYTHON) -m venv $(VIRTUALENV)
	source $(VIRTUALENV)/bin/activate; \
		$(SERIES_TIEMPO_PIP) install -r requirements.txt

anaconda_setup_etl_on_virtualenv: create_dir
	conda create -n $(CONDA_ENV) --no-default-packages
	source activate $(CONDA_ENV); \
		$(SERIES_TIEMPO_PIP) install -r requirements.txt

anaconda_update_environment: create_dir
	git pull origin master
	$(SERIES_TIEMPO_PIP) install -r requirements.txt --upgrade

anaconda_run_error:
	etl --log-level DEBUG

anaconda_run:
	etl

create_dir:
	mkdir -p logs
	mkdir -p docs
	mkdir -p data
	mkdir -p data/input
	mkdir -p data/input/catalog
	mkdir -p data/output
	mkdir -p data/output/catalog
	mkdir -p data/test_output
	mkdir -p data/test_output/catalog
	mkdir -p data/reports
	mkdir -p data/backup
	mkdir -p data/backup/catalog

clean:
	rm -rf data/input/
	rm -rf data/output/
	rm -rf data/test_output/
	rm -rf data/reports
	make create_dir

install:
	pipenv install

run:
	pipenv run etl
