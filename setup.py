#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

with open("requirements.txt") as f:
    requirements = [req.strip() for req in f.readlines()]

setup(
    name='series-tiempo-ar-scraping',
    version='0.3.1',
    description="Descripción corta del proyecto.",
    long_description=readme,
    author="Datos Argentina",
    author_email='datos@modernizacion.gob.ar',
    url='https://github.com/datosgobar/series-tiempo-ar-scraping',
    packages=[
        'series_tiempo_ar_scraping',
    ],
    package_dir={'series_tiempo_ar_scraping':
                 'series_tiempo_ar_scraping'},
    include_package_data=True,
    install_requires=requirements,
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
