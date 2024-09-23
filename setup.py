#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.md', encoding="utf-8") as readme_file:
    readme = readme_file.read()

with open("requirements.txt", encoding ="utf-8") as f:
    requirements = [req.strip() for req in f.readlines()]

setup(
    name='series-tiempo-ar-scraping',
    version='0.3.1',
    description="Descripción corta del proyecto.",
    long_description=readme,
    author="Datos Argentina",
    author_email='datosargentina@jefatura.gob.ar',
    url='https://github.com/datosgobar/series-tiempo-ar-scraping',
    packages=[
        'series_tiempo_ar_scraping',
    ],
    package_dir={'series_tiempo_ar_scraping':
                 'series_tiempo_ar_scraping'},
    include_package_data=True,
    install_requires=requirements,
    dependency_links=[
        'git+https://github.com/datosgobar/xlseries.git#egg=xlseries',
    ],
    entry_points={
        'console_scripts': [
            'etl=series_tiempo_ar_scraping.main:cli'
        ]
    },
    license="MIT license",
    zip_safe=False,
    keywords='series_tiempo_ar_scraping',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
    ],
)
