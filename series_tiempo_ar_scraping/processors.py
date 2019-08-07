import logging
import os
import pandas as pd
import re
from copy import deepcopy

from series_tiempo_ar.readers.csv_reader import CSVReader
import series_tiempo_ar.readers as readers
from series_tiempo_ar.validations import validate_distribution
from series_tiempo_ar.validations import validate_distribution_scraping
from series_tiempo_ar.readers.csv_reader import CSVReader

from xlseries import XlSeries
from xlseries.strategies.clean.parse_time import TimeIsNotComposed

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")


class BaseProcessor():

    def __init__(self, distribution_metadata):

        self.distribution_metadata = distribution_metadata

    def run(self):
        raise NotImplementedError


class DirectDownloadProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata

    def run(self):
        valid_df, distribution_df = False, None

        try:
            reader = CSVReader(self.distribution_metadata)
            valid_df, distribution_df = True, reader.read()
            logging.debug('>>> Descargó la distribución <<<')
        except Exception:
            logging.debug('>>> Falló la descarga de la distribución <<<')
            raise

        return distribution_df



class TXTProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata

    def run(self):
        distribution_df = None

        file_source = os.path.join(
            CATALOGS_DIR_INPUT,
            self.catalog_metadata.get('identifier'),
            'sources',
            self.distribution_metadata.get('scrapingFileURL').split('/')[-1]
        )

        try:
            distribution_df = self.catalog_metadata.load_ts_distribution(
                identifier=self.distribution_metadata.get('identifier'),
                catalog_id=self.catalog_metadata.get('identifier'),
                file_source=file_source
            )

            logging.debug('Descargó la distribución')

        except Exception:
            logging.debug('Falló la descarga de la distribución')
            raise

        return distribution_df


XLSERIES_PARAMS = {
    'alignment': 'vertical',
    'composed_headers_coord': None,
    'context': None,
    'continuity': True,
    'blank_rows': False,
    'missings': True,
    "missing_value": [
        None, "", " ", "-", "--", "---", ".", "...", "/", "///",
        "s.d.", "s.d", "s/d",
        "n,d,", "n,d", "n.d.", "n.d", "n/d",
        "s", "x"
    ],
    'time_alignment': 0,
    'time_multicolumn': False,
    "headers_coord": None,
    "data_starts": None,
    "frequency": None,
    "time_header_coord": None,
}

PRESERVE_WB_OBJ = False


class SpreadsheetProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata, catalog_context):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata
        self.catalog_context = catalog_context

    def run(self):
        distribution_df = None

        file_source = os.path.join(
            CATALOGS_DIR_INPUT,
            self.catalog_metadata.get('identifier'),
            'sources',
            self.distribution_metadata.get('scrapingFileURL').split('/')[-1]
        )

        catalog_sources_dir = os.path.join(
            CATALOGS_DIR_INPUT,
            self.catalog_metadata.get('identifier'),
            'sources',
        )

        try:
            xl = self.catalog_context['xl'].get(file_source.split('/')[-1])

            distribution_params = self.gen_distribution_params(
                self.catalog_metadata, self.distribution_metadata.get('identifier'))
            distrib_meta = self.catalog_metadata.get_distribution(self.distribution_metadata.get('identifier'))
            dataset_meta = self.catalog_metadata.get_dataset(self.distribution_metadata.get('identifier').split(".")[0])

            df = self.scrape_dataframe(xl, **distribution_params)

            if isinstance(df, list):
                df = pd.concat(df, axis=1)

            # VALIDACIONES
            worksheet = distribution_params["worksheet"]
            headers_coord = distribution_params["headers_coord"]
            headers_value = distribution_params["headers_value"]

            validate_distribution_scraping(xl, worksheet, headers_coord, headers_value,
                                           distrib_meta)
            validate_distribution(df, self.catalog_metadata, dataset_meta, distrib_meta,
                                  self.distribution_metadata.get('identifier'))

            return df

        except Exception:
            logging.debug('Falló la descarga de la distribución')
            raise

        return distribution_df

    def gen_distribution_params(self, catalog, distribution_identifier):
        distribution = catalog.get_distribution(distribution_identifier)

        fields = distribution["field"]
        params = {}

        # hoja de la Distribucion
        params["worksheet"] = distribution["scrapingFileSheet"]

        # coordenadas de los headers de las series
        params["headers_coord"] = [
            field["scrapingIdentifierCell"]
            for field in fields
            if not field.get("specialType")
        ]

        # coordenadas de los headers de las series
        params["headers_value"] = [field["id"] for field in fields
                                   if not field.get("specialType")]

        # fila donde empiezan los datos
        params["data_starts"] = list(map(
            self.row_from_cell_coord,
            [field["scrapingDataStartCell"] for field in fields
             if not field.get("specialType")]))

        # frecuencia de las series
        field = catalog.get_field(distribution_identifier=distribution_identifier,
                                  title="indice_tiempo")
        params["frequency"] = self.freq_iso_to_xlseries(
            field["specialTypeDetail"])

        # coordenadas del header del indice de tiempo
        params["time_header_coord"] = field["scrapingIdentifierCell"]

        # nombres de las series
        params["series_names"] = [
            f["title"] for f in fields
            if not f.get("specialType")
        ]

        return params

    def scrape_dataframe(self, xl, worksheet, headers_coord, headers_value, data_starts,
                         frequency, time_header_coord, series_names):
        params = deepcopy(XLSERIES_PARAMS)
        params["headers_coord"] = headers_coord
        params["data_starts"] = data_starts
        params["frequency"] = frequency
        params["time_header_coord"] = time_header_coord
        params["series_names"] = series_names

        try:
            params["time_composed"] = True
            dfs = xl.get_data_frames(deepcopy(params), ws_name=worksheet,
                                     preserve_wb_obj=PRESERVE_WB_OBJ)
        except TimeIsNotComposed:
            params["time_composed"] = False
            dfs = xl.get_data_frames(deepcopy(params), ws_name=worksheet,
                                     preserve_wb_obj=PRESERVE_WB_OBJ)

        return dfs

    def row_from_cell_coord(self, coord):
        match = re.match(r'^[A-Za-z]+(\d+)$', coord)
        if not match:
            raise ValueError('Invalid coordinate')

        return int(match.group(1))

    def freq_iso_to_xlseries(self, freq_iso8601):
        frequencies_map = {
            "R/P1Y": "Y",
            "R/P6M": "S",
            "R/P3M": "Q",
            "R/P1M": "M",
            "R/P1D": "D"
        }
        return frequencies_map[freq_iso8601]
