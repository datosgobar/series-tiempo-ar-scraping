# Makefile para Ubuntu 16.04
SHELL = bash
SERIES_TIEMPO_PIP ?= pip
SERIES_TIEMPO_PYTHON ?= python
VIRTUALENV = series-tiempo-ar-scraping
CONDA_ENV = series-tiempo-ar-scraping

.PHONY: all clean create_dir install_anaconda run setup_anaconda

all: run

clean:
	rm -rf data/input/
	rm -rf data/output/
	rm -rf data/test_output/
	rm -rf data/reports
	make create_dir

create_dir:
	mkdir -p logs
	mkdir -p docs
	mkdir -p scripts
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

install_anaconda:
	wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
	bash Miniconda3-latest-Linux-x86_64.sh
	rm Miniconda3-latest-Linux-x86_64.sh

run:
	source activate $(CONDA_ENV); etl

setup_anaconda:
	conda create -n $(CONDA_ENV) python=3.6 --no-default-packages
	source activate $(CONDA_ENV); $(SERIES_TIEMPO_PIP) install -e .

setup_virtualenv: create_dir
	test -d $(VIRTUALENV)/bin/activate || $(SERIES_TIEMPO_PYTHON) -m venv $(VIRTUALENV)
	source $(VIRTUALENV)/bin/activate; \
		$(SERIES_TIEMPO_PIP) install -r requirements.txt

update_environment: create_dir
	git pull origin master
	$(SERIES_TIEMPO_PIP) install -r requirements.txt --upgrade
