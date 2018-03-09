# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function, with_statement
from __future__ import absolute_import

import requests
import time

DEFAULT_TRIES = 1
RETRY_DELAY = 1


def download(url, tries=DEFAULT_TRIES, retry_delay=RETRY_DELAY,
             try_timeout=None, proxies=None, verify=True):
    for i in range(tries):
        try:
            return requests.get(url, timeout=try_timeout, proxies=proxies,
                                verify=verify).content
        except Exception as e:
            download_exception = e

            if i < tries - 1:
                time.sleep(retry_delay)

    raise download_exception


def download_to_file(url, file_path, **kwargs):
    content = download(url, **kwargs)
    with open(file_path, "wb") as f:
        f.write(content)
