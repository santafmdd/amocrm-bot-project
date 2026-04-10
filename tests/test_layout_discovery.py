import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import unittest

from src.writers.layout_discovery import discover_stage_blocks_from_matrix


class TestLayoutDiscovery(unittest.TestCase):
    def test_discover_stage_block_from_matrix(self):
        matrix = [
            ["", ""],
            ["Машэкспо: Даты=Созданы; Период=За все время; Теги=машэкспо", ""],
            ["Этап", "Все (шт)", "Активные (шт)", "Закрытые (шт)"],
            ["ВЕРИФИКАЦИЯ", "10", "7", "3"],
            ["ПЕРВЫЙ КОНТАКТ. КВАЛИФИКАЦИЯ", "5", "4", "1"],
        ]
        blocks = discover_stage_blocks_from_matrix(matrix)
        self.assertEqual(len(blocks), 1)
        b = blocks[0]
        self.assertEqual(b.header_row, 2)
        self.assertEqual(b.stage_col, 1)
        self.assertEqual(b.all_col, 2)
        self.assertEqual(b.active_col, 3)
        self.assertEqual(b.closed_col, 4)


if __name__ == "__main__":
    unittest.main()
