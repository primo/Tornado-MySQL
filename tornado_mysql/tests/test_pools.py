import time
from tornado.testing import gen_test
from tornado import gen


from tornado_mysql.err import OperationalError
from tornado_mysql import pools
from tornado_mysql.tests import base


class TestPool(base.PyMySQLTestCase):
    def tearDown(self):
        pass

    @gen_test(timeout=60)
    def test_single_connection(self):
        connect_kwargs = self.databases[0].copy()
        pool = pools.Pool(connect_kwargs, max_open_connections=1)

        # Ok.
        cur = yield pool.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], self.databases[0]["db"])

        yield pool.execute("SET wait_timeout=1")
        time.sleep(2)

        with self.assertRaises(OperationalError) as e:
            # "MySQL has gone away".
            yield pool.execute('SELECT database()')

        self.assertEqual(pool._opened_conns, 0)

        pool.connect_kwargs["host"] = "foo"
        with self.assertRaises(OperationalError):
            # "Can't connect".
            yield pool.execute('SELECT database()')

        pool.connect_kwargs["host"] = self.databases[0]["host"]

        # Should connect back.
        cur = yield pool.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], self.databases[0]["db"])

    @gen_test(timeout=60)
    def test_multiple_connections(self):
        connect_kwargs = self.databases[0].copy()
        pool = pools.Pool(connect_kwargs, max_open_connections=2)

        # Ok.
        cur = yield pool.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], self.databases[0]["db"])

        yield pool.execute("SET wait_timeout=1")
        time.sleep(2)

        with self.assertRaises(OperationalError):
            # "MySQL has gone away".
            yield pool.execute('SELECT database()')

        self.assertEqual(pool._opened_conns, 0)

        pool.connect_kwargs["host"] = "foo"
        for _ in range(2):
            with self.assertRaises(OperationalError) as e:
                # "Can't connect".
                yield pool.execute('SELECT database()')

        pool.connect_kwargs["host"] = self.databases[0]["host"]

        # Should connect back.
        for _ in range(2):
            cur = yield pool.execute('SELECT database()')
            self.assertEqual(cur.fetchone()[0], self.databases[0]["db"])
