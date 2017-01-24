import tempstore.engine as ts_e

import unittest

SECONDS = 1
MINUTES = 60 * SECONDS
HOURS = 60 * MINUTES
DAYS = 24 * HOURS

class TestFormatExpiry(unittest.TestCase):

    def test_format_expiry(self):

        # Expired
        self.assertEqual(
            ts_e.format_expiry(-5 * SECONDS),
            'expired')
        self.assertEqual(
            ts_e.format_expiry(0),
            'expired')

        # Expires in a few seconds
        self.assertEqual(
            ts_e.format_expiry(1 * SECONDS),
            'expires in 1 second')
        self.assertEqual(
            ts_e.format_expiry(5 * SECONDS),
            'expires in 5 seconds')

        # Expires in around 1 minute
        self.assertEqual(
            ts_e.format_expiry(1 * MINUTES - 5 * SECONDS),
            'expires in 55 seconds')
        self.assertEqual(
            ts_e.format_expiry(1 * MINUTES),
            'expires in 1 minute')
        self.assertEqual(
            ts_e.format_expiry(1 * MINUTES + 5 * SECONDS),
            'expires in 1 minute')

        # Expires in a around 2 minutes
        self.assertEqual(
            ts_e.format_expiry(2 * MINUTES - 5 * SECONDS),
            'expires in 2 minutes')
        self.assertEqual(
            ts_e.format_expiry(2 * MINUTES),
            'expires in 2 minutes')
        self.assertEqual(
            ts_e.format_expiry(2 * MINUTES + 5 * SECONDS),
            'expires in 2 minutes')

        # Expires in around 1 hour
        self.assertEqual(
            ts_e.format_expiry(1 * HOURS - 5 * MINUTES),
            'expires in 55 minutes')
        self.assertEqual(
            ts_e.format_expiry(1 * HOURS),
            'expires in 1 hour')
        self.assertEqual(
            ts_e.format_expiry(1 * HOURS + 5 * MINUTES),
            'expires in 1 hour')

        # Expires in around 2 hours
        self.assertEqual(
            ts_e.format_expiry(2 * HOURS - 5 * MINUTES),
            'expires in 2 hours')
        self.assertEqual(
            ts_e.format_expiry(2 * HOURS),
            'expires in 2 hours')
        self.assertEqual(
            ts_e.format_expiry(2 * HOURS + 5 * MINUTES),
            'expires in 2 hours')

        # Expires in around 1 day
        self.assertEqual(
            ts_e.format_expiry(1 * DAYS - 4 * HOURS),
            'expires in 20 hours')
        self.assertEqual(
            ts_e.format_expiry(1 * DAYS),
            'expires in 1 day')
        self.assertEqual(
            ts_e.format_expiry(1 * DAYS + 4 * HOURS),
            'expires in 1 day')

        # Expires in around 2 days
        self.assertEqual(
            ts_e.format_expiry(2 * DAYS - 4 * HOURS),
            'expires in 2 days')
        self.assertEqual(
            ts_e.format_expiry(2 * DAYS),
            'expires in 2 days')
        self.assertEqual(
            ts_e.format_expiry(2 * DAYS + 4 * HOURS),
            'expires in 2 days')
