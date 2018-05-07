# -*- coding: utf-8 -*-

import os
import unittest
import shutil
import io
import responses

from scripts import paths


def test_files_dir(*args):
    return os.path.join("tests", "test_files", *args)


class TestBase(unittest.TestCase):
    def setUp(self):
        if "TESTING" not in os.environ or paths.ROOT_DIR == paths.PROJECT_DIR:
            raise Exception("TESTING environent variable not set.")

        if not hasattr(self, "_name") or not self._name:
            raise Exception("Test _name not set.")

        # Borrar el directorio de proyecto de testing
        if os.path.isdir(paths.PROJECT_DIR):
            shutil.rmtree(paths.PROJECT_DIR)

        # Crear un nuevo directorio de proyecto para testing
        shutil.copytree(test_files_dir(self._name), paths.PROJECT_DIR)

        # Copiar archivos JSON Schema
        shutil.copytree(os.path.join(paths.ROOT_DIR, "config", "schemas"),
                        os.path.join(paths.PROJECT_DIR, "config", "schemas"))

        # TODO: COPY SCHEMAS
        # shutil.copytree()
        
        # Crear directorios vacíos (equivalente a make create_dir)
        for path in [paths.DATOS_DIR,
                     paths.REPORTES_DIR,
                     paths.CATALOGS_DIR,
                     paths.CATALOGS_DIR_INPUT]:
            if not os.path.isdir(path):
                os.makedirs(path)


class MockDownloads(object):
    def __init__(self):
        self._mocker = responses.RequestsMock()
        self._files = []
        pass

    def add_url_files(self, url_files):
        for url, path in url_files:
            f = io.open(path, "rb")
            self._files.append(f)
            self._mocker.add(responses.GET, url, body=f)

    def add_url_errors(self, url_errors):
        for url, code in url_errors:
            self._mocker.add(responses.GET, url, status=code)

    def start(self):
        self._mocker.start()

    def stop(self):
        self._mocker.stop()
        self._mocker.reset()

        for f in self._files:
            f.close()

    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, type, value, traceback):
        self.stop()
        return False