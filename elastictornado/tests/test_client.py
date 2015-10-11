import unittest

from elastictornado import ElasticTornado


class TestClient(unittest.TestCase):
    def test_stub(self):
        et = ElasticTornado('http://localhost:9200/')
        print et
        self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
