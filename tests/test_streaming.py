import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app


class StreamingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        app.DATABASE = self.temp.name + "/stream.db"
        app.WORKER_PAUSED.clear()
        app.initialize_database()

    def tearDown(self):
        self.temp.cleanup()

    def create(self, name="Keyboard"):
        return app.create_product(app.ProductCreate(name=name, category="Hardware", price=99, stock=5))

    def checkpoint(self):
        with app.db() as connection:
            return dict(connection.execute("SELECT * FROM consumer_checkpoint").fetchone())

    def test_ordered_insert_update_delete(self):
        product = self.create()
        app.update_product(product["id"], app.ProductUpdate(stock=9))
        app.delete_product(product["id"])
        self.assertTrue(app.consume_one())
        self.assertTrue(app.consume_one())
        self.assertTrue(app.consume_one())
        self.assertEqual(self.checkpoint()["last_sequence"], 3)
        with app.db() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM read_products").fetchone()[0], 0)

    def test_duplicate_delivery_is_skipped(self):
        self.create()
        app.consume_one()
        app.consumer_action("duplicate")
        app.consume_one()
        checkpoint = self.checkpoint()
        self.assertEqual(checkpoint["processed"], 1)
        self.assertEqual(checkpoint["duplicates"], 1)

    def test_pause_accumulates_lag_and_resume_recovers(self):
        app.WORKER_PAUSED.set()
        self.create("Mouse")
        self.assertFalse(app.consume_one())
        self.assertEqual(self.checkpoint()["last_sequence"], 0)
        app.WORKER_PAUSED.clear()
        self.assertTrue(app.consume_one())
        self.assertEqual(self.checkpoint()["last_sequence"], 1)

    def test_snapshot_rebuild_replays_latest_state(self):
        first = self.create("Monitor")
        second = self.create("Headphones")
        app.update_product(first["id"], app.ProductUpdate(price=120))
        while app.consume_one():
            pass
        app.consumer_action("rebuild")
        while app.consume_one():
            pass
        with app.db() as connection:
            rows = connection.execute("SELECT * FROM read_products ORDER BY name").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(next(row for row in rows if row["id"] == first["id"])["price"], 120)
            self.assertTrue(any(row["id"] == second["id"] for row in rows))


if __name__ == "__main__":
    unittest.main()
