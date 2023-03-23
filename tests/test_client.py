from patata import Patata


def test_dummy():
    client = Patata()
    assert client
    client.close()
