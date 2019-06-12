import logging
import os

from series_tiempo_ar_scraping.download import download_to_file

logging.basicConfig(level=logging.DEBUG)


class DirectDownloadProcessor():

    def __init__(self, download_url, distribution_dir, distribution_path, *args, **kwargs):
        self.download_url = download_url
        self.distribution_dir = distribution_dir
        self.distribution_path = distribution_path

    def ensure_dir_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def run(self):
        try:
            # import pdb; pdb.set_trace()
            # print(self.context.get("distribution_dir"))
            # print(self.context.get("distribution_path"))
            self.ensure_dir_exists(self.distribution_dir)
            download_to_file(
                url=self.download_url,
                file_path=self.distribution_path,
            )
        except:
            logging.debug('>>> Falló la descarga de la distribución <<<')

        # try:
        #     distrib_meta = self.context.get("meta")
        #     distribution_name = meta.get("title")
        #     distribution_file_name = meta.get("fileName", "{}.csv".format(meta.get("name")))
        #     dist_download_dir = os.path.join(
        #         dataset_dir, "distribution", distribution_identifier,
        #         "download"
        #     )
        #     dist_path = os.path.join(dist_download_dir,
        #                              "{}".format(distribution_file_name))
        #     dist_url = get_distribution_url(dist_path)

        #     distrib_meta["downloadURL"] = dist_url

        #     # chequea si ante la existencia del archivo hay que reemplazarlo o
        #     # saltearlo
        #     if not os.path.exists(dist_path) or replace:
        #         status = "Replaced" if os.path.exists(dist_path) else "Created"
        #         distribution = scrape_distribution(
        #             xl, catalog, distribution_identifier)

        #         if isinstance(distribution, list):
        #             distribution_complete = pd.concat(distribution)
        #         else:
        #             distribution_complete = distribution

        #         helpers.remove_other_files(os.path.dirname(dist_path))
        #         distribution_complete.to_csv(
        #             dist_path, encoding="utf-8",
        #             index_label="indice_tiempo")
        #     else:
        #         status = "Skipped"

        #     res["distributions_ok"].append((distribution_identifier, status))
        #     logger.info(msg.format(distribution_identifier, "OK", status))

        # except Exception as e:
        #     pass