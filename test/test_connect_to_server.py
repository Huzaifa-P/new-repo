from utils.connect_to_server import connect_to_server


def test_connect_to_server_with_mock(mocker):

    mock_psycopg2_connect = mocker.patch('psycopg2.connect')

    expected_connection = mocker.MagicMock()
    mock_psycopg2_connect.return_value = expected_connection

    credentials = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password"
    }

    connection = connect_to_server(credentials)

    mock_psycopg2_connect.assert_called_once_with(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="test_user",
        password="test_password"
    )

    assert connection == expected_connection
