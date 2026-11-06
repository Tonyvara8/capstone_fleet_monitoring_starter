import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("alerts_notifier", Path(__file__).resolve().parents[1] / "Alerts" / "alerts_notifier.py")
notifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(notifier)


class SeenAlertsTest(unittest.TestCase):
    def test_second_copy_of_an_alert_is_detected(self):
        seen = notifier.SeenAlerts()
        self.assertFalse(seen.already_seen("e1:speeding"))
        self.assertTrue(seen.already_seen("e1:speeding"))
        self.assertFalse(seen.already_seen("e1:harsh_braking"))

    def test_memory_is_bounded(self):
        seen = notifier.SeenAlerts(capacity=2)
        for alert_id in ("a", "b", "c"):
            seen.already_seen(alert_id)
        self.assertFalse(seen.already_seen("a"))
        self.assertTrue(seen.already_seen("c"))

    def test_alert_without_id_is_never_a_duplicate(self):
        seen = notifier.SeenAlerts()
        self.assertFalse(seen.already_seen(None))
        self.assertFalse(seen.already_seen(None))

    def test_handle_alert_rejects_non_objects(self):
        with self.assertRaises(TypeError):
            notifier.handle_alert([1, 2])


if __name__ == "__main__":
    unittest.main()
