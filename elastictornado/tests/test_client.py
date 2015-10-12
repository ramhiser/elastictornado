from tornado.testing import AsyncTestCase

from elastictornado import ElasticTornado


class TestClient(AsyncTestCase):
    def test_stub(self):
        et = ElasticTornado('http://localhost:9200/')
        print et
        self.assertTrue(False)

