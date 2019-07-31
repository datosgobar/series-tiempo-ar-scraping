import logging
import os

import series_tiempo_ar.readers as readers

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
            distribution_df = self.catalog_metadata.load_ts_distribution(
                self.distribution_metadata.get('identifier'),
                self.catalog_metadata.get('identifier'),
                file_source=self.distribution_metadata.get('downloadURL')
            )
            logging.debug('Descargó la distribución')
        except Exception:
            logging.debug('Falló la descarga de la distribución')
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
            distribution_df = readers.load_ts_distribution(
                self.catalog_metadata,
                self.distribution_metadata.get('identifier'),
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
    # 'time_composed': True
}


class SpreadsheetProcessor(BaseProcessor):

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
            # TODO: Change me!

            # distribution_params = gen_distribution_params(
            #     catalog, distribution_identifier)
            # distrib_meta = catalog.get_distribution(distribution_identifier)
            # dataset_meta = catalog.get_dataset(distribution_identifier.split(".")[0])

            # df = scrape_dataframe(xl, **distribution_params)

            # if isinstance(df, list):
            #     df = pd.concat(df, axis=1)

            # # VALIDACIONES
            # worksheet = distribution_params["worksheet"]
            # headers_coord = distribution_params["headers_coord"]
            # headers_value = distribution_params["headers_value"]

            # validate_distribution_scraping(xl, worksheet, headers_coord, headers_value,
            #                                distrib_meta)
            # validate_distribution(df, catalog, dataset_meta, distrib_meta,
            #                       distribution_identifier)

            # return df <<<--- nuestro return

            # distribution_df = readers.load_ts_distribution(
            #     self.catalog_metadata,
            #     self.distribution_metadata.get('identifier'),
            #     file_source=file_source
            # )

            logging.debug('Descargó la distribución')

        except Exception:
            logging.debug('Falló la descarga de la distribución')
            raise

        return distribution_df

    def gen_distribution_params(catalog, distribution_identifier):
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
            helpers.row_from_cell_coord,
            [field["scrapingDataStartCell"] for field in fields
             if not field.get("specialType")]))

        # frecuencia de las series
        field = catalog.get_field(distribution_identifier=distribution_identifier,
                                  title="indice_tiempo")
        params["frequency"] = helpers.freq_iso_to_xlseries(
            field["specialTypeDetail"])

        # coordenadas del header del indice de tiempo
        params["time_header_coord"] = field["scrapingIdentifierCell"]

        # nombres de las series
        params["series_names"] = [
            f["title"] for f in fields
            if not f.get("specialType")
        ]

        return params

    def scrape_dataframe(xl, worksheet, headers_coord, headers_value, data_starts,
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
