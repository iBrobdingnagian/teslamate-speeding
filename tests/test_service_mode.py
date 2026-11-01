import collections
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import alert_threads
import service_mode


class Monitor(alert_threads.ThreadsMixin):
    def __init__(self):
        self.alert_history, self.alert_counts = [], collections.Counter()
        self.telegram_successes = self.telegram_failures = 0
        self.car_data = {'1': {}}
        self.threads_init('tok', 'chat', 100)

    def save_state(self):
        pass


class ServiceModeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for p in (patch.object(service_mode, 'FLAG_FILE', Path(self.tmp.name) / 'service_mode.json'),
                  patch.object(alert_threads, 'ALERTS_DRY_RUN', False),
                  patch.object(alert_threads.time, 'sleep'),
                  patch.dict(os.environ, {'PAGERDUTY_ROUTING_KEY': 'k'})):
            p.start()
            self.addCleanup(p.stop)
        self.post = patch.object(alert_threads.requests, 'post').start()
        self.addCleanup(patch.stopall)
        self.post.return_value = MagicMock(status_code=202, json=lambda: {'ok': True, 'result': {'message_id': 7}})
        self.m = Monitor()

    def test_flag_roundtrip(self):
        self.assertFalse(service_mode.is_service_mode())
        self.assertIsNone(service_mode.set_service_mode(True))
        self.assertTrue(service_mode.is_service_mode())
        self.assertIsNotNone(service_mode.set_service_mode(True))   # idempotent, keeps first timestamp
        self.assertIsNotNone(service_mode.set_service_mode(False))
        self.assertFalse(service_mode.is_service_mode())
        service_mode.set_service_mode(False)                        # off while off is fine

    def test_broken_flag_counts_as_off(self):
        service_mode.FLAG_FILE.write_text('not json')
        self.assertFalse(service_mode.is_service_mode())

    def test_muted_while_on_and_live_after(self):
        service_mode.set_service_mode(True)
        self.assertEqual(self.m._pagerduty('trigger', 'd', 'summary'), 'muted (service mode)')
        self.assertTrue(self.m.tg_send('hello'))
        self.post.assert_not_called()
        service_mode.set_service_mode(False)
        self.assertEqual(self.m._pagerduty('trigger', 'd', 'summary'), 'sent')
        self.assertEqual(self.m.tg_send('hello'), 7)

    def test_safety_condition_pages_after_service_mode_ends(self):
        cond = {'tyre_fl': (True, 'Front left tyre low', {})}
        service_mode.set_service_mode(True)
        self.m._dispatch_safety('1', cond, now=1000)
        self.post.assert_not_called()
        self.assertFalse(self.m.safety_calls['1:tyre_fl']['active'])
        service_mode.set_service_mode(False)
        self.m._dispatch_safety('1', cond, now=1100)
        self.assertTrue(self.m.safety_calls['1:tyre_fl']['active'])
        self.assertEqual(self.post.call_count, 1)

    def test_charge_call_not_latched_while_muted(self):
        self.m.car_data['1'] = {'plugged_in': 'true'}
        service_mode.set_service_mode(True)
        self.m._charge_call_tick('1', 1000)
        self.assertNotIn('1', self.m.charge_calls)


if __name__ == '__main__':
    unittest.main()
