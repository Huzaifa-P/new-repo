from moto import mock_secretsmanager
import boto3
import json
from pytest import raises
import logging

from utils.get_secret import get_secret


@mock_secretsmanager
class TestGetSecret:
    def test_get_secret_will_retrieve_list_of_secrets(self):
        secret_manager = boto3.client(
            'secretsmanager', region_name='eu-west-2')

        secret_key_value = json.dumps(
            {'user': 'Hasan', 'password': 'Password'})
        secret_manager.create_secret(
            Name='secret',
            SecretString=secret_key_value
        )

        result = get_secret('secret')
        expected = {'user': 'Hasan', 'password': 'Password'}

        assert result == expected

    def test_get_secret_raise_an_error_when_no_secret_exists_for_name_passed_to_func(
            self):
        with raises(Exception):
            get_secret('secret')

    def test_get_secret_logs_a_debug_when_no_secret_exists_for_name_passed_to_func(
            self,
            caplog):
        with raises(Exception):
            with caplog.at_level(logging.DEBUG):
                get_secret('secret')
        # or, if you really need to check the log-level
        assert caplog.records[-1].levelname == "DEBUG"
        assert caplog.records[-1].message == "This secret doesn't exist."
