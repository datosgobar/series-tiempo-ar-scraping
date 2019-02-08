from unittest import mock
from scripts import extract_catalogs, scrape_datasets, send_email
from . import TestBase, MockDownloads, test_files_dir


class TestSendEmailExtraction(TestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name = "send_email_extraction"
        self._mocker = None
        self._patcher = None

    def setUp(self):
        self._patcher = mock.patch('smtplib.SMTP_SSL', autospec=True)
        self._smtp = self._patcher.start()
        super().setUp()

    def tearDown(self):
        self._patcher.stop()
        super().tearDown()

    def test_extraction_emails(self):
        """
        Luego de una extracción de datos, se debería enviar correos a los
        destinatarios correspondientes.
        """
        self._mocker = MockDownloads()
        self._mocker.add_url_files([
            ("https://example.com/test2.xlsx",
             test_files_dir(self._name, "mock", "test2.xlsx")),
            ("https://example.com/test1.json",
             test_files_dir(self._name, "mock", "test1.json"))
        ])
        self._mocker.start()

        extract_catalogs.main()
        send_email.send_group_emails("extraccion")

        smtp_client = self._smtp()
        sendmail = smtp_client.sendmail
        recipients = []

        for call, *_ in sendmail.call_args_list:
            recipients.extend(call[1])

        self.assertSetEqual(set(recipients), {
            'json-url@example.com',
            'xlsx-url@example.com',
            'json-local@example.com',
            'xlsx-local@example.com'
        })


class TestSendEmailScrape(TestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name = "send_email_scrape"
        self._patcher = None

    def setUp(self):
        self._patcher = mock.patch('smtplib.SMTP_SSL', autospec=True)
        self._smtp = self._patcher.start()
        super().setUp()

    def tearDown(self):
        self._patcher.stop()
        super().tearDown()

    def test_scraping_emails(self):
        """
        Luego de un scraping de catálogos, se debería enviar correos a los
        destinatarios correspondientes.
        """
        scrape_datasets.main(True)
        send_email.send_group_emails("scraping")

        smtp_client = self._smtp()
        sendmail = smtp_client.sendmail
        recipients = []

        for call, *_ in sendmail.call_args_list:
            recipients.extend(call[1])

        self.assertListEqual(recipients, [
            'jane@example.com', 'john@example.com'
        ])
