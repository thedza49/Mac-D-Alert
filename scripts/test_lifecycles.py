#!/usr/bin/env python3
"""
scripts/test_lifecycles.py
TDD Validation for Signal Lifecycle Engine (v1.6.0)
Momo (Lead Builder)
"""

import unittest
import sqlite3
import os
import sys
from unittest.mock import patch, MagicMock

# Add the root directory to path so we can import analyze_lifecycles and database_helper
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.analyze_lifecycles import calculate_metrics, pair_signals, discover_apex

class TestSignalLifecycle(unittest.TestCase):
    
    def test_calculate_metrics(self):
        print("\n--- Testing Metric Calculations ---")
        
        # 1. Standard Profit: Buy: $100 | Apex: $120 | Sell: $110
        # Result: 10% Captured, 20% Peak, 50% Efficiency.
        metrics = calculate_metrics(100.0, 120.0, 110.0)
        print(f"Standard Profit: {metrics}")
        self.assertAlmostEqual(metrics['captured_profit'], 0.1)
        self.assertAlmostEqual(metrics['peak_profit'], 0.2)
        self.assertAlmostEqual(metrics['capture_efficiency'], 0.5)
        self.assertEqual(metrics['is_win'], 1)
        
        # 2. Bag-Hold Loss: Buy: $100 | Apex: $110 | Sell: $90
        # Result: -10% Captured, 10% Peak, -100% Efficiency.
        metrics = calculate_metrics(100.0, 110.0, 90.0)
        print(f"Bag-Hold Loss: {metrics}")
        self.assertAlmostEqual(metrics['captured_profit'], -0.1)
        self.assertAlmostEqual(metrics['peak_profit'], 0.1)
        self.assertAlmostEqual(metrics['capture_efficiency'], -1.0)
        self.assertEqual(metrics['is_win'], 0)
        
        # 3. Perfect Exit: Buy: $100 | Apex: $115 | Sell: $115
        # Result: 15% Captured, 15% Peak, 100% Efficiency.
        metrics = calculate_metrics(100.0, 115.0, 115.0)
        print(f"Perfect Exit: {metrics}")
        self.assertAlmostEqual(metrics['captured_profit'], 0.15)
        self.assertAlmostEqual(metrics['peak_profit'], 0.15)
        self.assertAlmostEqual(metrics['capture_efficiency'], 1.0)
        self.assertEqual(metrics['is_win'], 1)

    @patch('scripts.analyze_lifecycles.get_connection')
    def test_pairing_and_open_trade(self, mock_get_conn):
        print("\n--- Testing Signal Pairing & Open Trade Safety ---")
        
        # Setup mock DB for pairing test
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # 4. Open Trade Safety: Buy exists, no Sell.
        # Should result in 0 pairs.
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'signal_date': '2023-01-01', 'signal_type': 'BUY', 'price_at_signal': 100.0}
        ]
        pairs = pair_signals("MOCK")
        print(f"Open Trade Safety (1 BUY, 0 SELL): {len(pairs)} pairs")
        self.assertEqual(len(pairs), 0)
        
        # 5. Full Pair
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'signal_date': '2023-01-01', 'signal_type': 'BUY', 'price_at_signal': 100.0},
            {'id': 2, 'signal_date': '2023-01-05', 'signal_type': 'SELL', 'price_at_signal': 110.0}
        ]
        pairs = pair_signals("MOCK")
        print(f"Standard Pair (1 BUY, 1 SELL): {len(pairs)} pair(s)")
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]['buy']['id'], 1)
        self.assertEqual(pairs[0]['sell']['id'], 2)

    @patch('scripts.analyze_lifecycles.get_connection')
    def test_apex_discovery(self, mock_get_conn):
        print("\n--- Testing Apex Discovery ---")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.fetchone.return_value = {'apex': 125.0}
        apex = discover_apex("MOCK", "2023-01-01", "2023-01-05")
        print(f"Discovered Apex: {apex}")
        self.assertEqual(apex, 125.0)

if __name__ == "__main__":
    unittest.main()
