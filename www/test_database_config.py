import unittest

from www.database_config import build_database_uri, database_engine_options


class DatabaseConfigurationTests(unittest.TestCase):
    def test_postgres_ssl_parameter_is_appended_safely(self):
        self.assertEqual(
            build_database_uri("postgresql://user:pass@db/app?connect_timeout=5"),
            "postgresql://user:pass@db/app?connect_timeout=5&sslmode=disable",
        )

    def test_existing_ssl_parameter_is_preserved(self):
        url = "postgresql://user:pass@db/app?sslmode=require"
        self.assertEqual(build_database_uri(url), url)

    def test_sqlite_smoke_tests_do_not_receive_postgres_pool_options(self):
        self.assertEqual(
            database_engine_options("sqlite:///:memory:"),
            {"pool_pre_ping": True},
        )


if __name__ == "__main__":
    unittest.main()
