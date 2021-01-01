import json

from jsonschema import validate, ValidationError

from jobs import utils
from tests.pyspark_base import PySparkTest


class TestParseJson(PySparkTest):
    def setUp(self) -> None:
        super().setUp()

        self.expected_result_not_overwrite = [{
            'day': '06',
            'json': '{"name":"third-parties-api","hostname":"ip-10-3-4-82.ec2.internal","pid":64,"level":30,"trace":{"request_id":"27d9668c-5e5a-4402-a314-74ac169faa9f","mach_id":"","device_id":""},"notification":{"transactionDetails":{"primaryAccountNumber":"xxxx-xxxx-xxxx-9718","retrievalReferenceNumber":"004821228142","userIdentifier":"cd1706ff-1706-4c54-bd06-6bbe61f4405f","cardholderBillAmount":8990,"billerCurrencyCode":"152","transactionID":"460048758524908","requestReceivedTimeStamp":"2020-02-17 '
                    '21:04:12","merchantInfo":{"name":"NETFLIX.COM","addressLines":["NETFLIX.COM"],"city":"Amsterdam","region":" '
                    '","countryCode":"NLD","merchantCategoryCode":"4899","currencyCode":"152","transactionAmount":8990}},"transactionOutcome":{"ctcDocumentID":"ctc-vn-531ad27f-0e7a-48be-8aac-ea5c501152b5","transactionApproved":"DECLINED","decisionResponseTimeStamp":"2020-02-17 '
                    '21:04:12","decisionID":"ctc-vn-2d1012a5-3c8f-469b-a2c4-37006014e098","notificationID":"ctc-vn-c0df0d8a-8543-4f33-b521-b8384055e2bd","alertDetails":[{"triggeringAppID":"85fbfc2e-6fa1-4c79-85db-772a5486a335","ruleCategory":"PCT_TRANSACTION","ruleType":"TCT_AUTO_PAY","alertReason":"DECLINE_BY_ISSUER","userIdentifier":"cd1706ff-1706-4c54-bd06-6bbe61f4405f","controlTargetType":"CARD_LEVEL"}]},"transactionTypes":["TCT_AUTO_PAY"],"sponsorId":"DPS_67959088580","appId":"85fbfc2e-6fa1-4c79-85db-772a5486a335"},"prepaidCard":{"state":"active","holderNumber":2,"_id":"5d1a8c67c27ab0003f7c9f14","machId":"cd1706ff-1706-4c54-bd06-6bbe61f4405f","createdAt":"2019-07-01T22:42:47.308Z","updatedAt":"2019-07-01T22:42:49.026Z","__v":0,"clientNumber":"00613180","contract":"00160001000000613181","holderName":"LEONARDO '
                    'ARTURO MORA '
                    'GAJARDO","expirationMonth":"07","expirationYear":"2027","last4Pan":"9718","id":"5d1a8c67c27ab0003f7c9f14"},"msg":"third_parties_visa_notification_delivery_callback_webhook","time":"2020-02-17T21:04:13.277Z","v":0}',
            'month': '07',
            'name': '30',
            'year': '2020'
        }]

        self.expected_result_overwrite = [{'name': '30'}]

    def test_json_extract_not_overwrite(self):
        logs = self.spark.read.csv('tests/resources/utils/Logs.csv', quote="'", header=True)
        json_schema = {
            'name': ['level']
        }
        column_name = 'json'

        test = utils.json_extract(logs, column_name, json_schema)

        self.assertEqual(self.df_to_dict(test), self.expected_result_not_overwrite)

    def test_json_extract_overwrite(self):
        logs = self.spark.read.csv('tests/resources/utils/Logs.csv', quote="'", header=True)
        json_schema = {
            'name': ['level']
        }
        column_name = 'json'
        test = utils.json_extract(logs, column_name, json_schema, overwrite=True)

        self.assertEqual(self.df_to_dict(test), self.expected_result_overwrite)

    @staticmethod
    def read_json_file(name):
        with open(f'tests/resources/utils/parse_json/{name}.json', 'r') as schema_file:
            return json.loads(schema_file.read())

    def test_check_schema(self):
        self.assertIsNone(utils.check_schema(self.read_json_file('schema')))

    def test_input_data(self):
        self.assertIsNone(utils.check_data(self.read_json_file('input1'), self.read_json_file('schema')))

    def test_input_data_error(self):
        self.assertRaises(
            ValidationError,
            utils.check_data,
            self.read_json_file('input1_error'),
            self.read_json_file('schema')
        )
