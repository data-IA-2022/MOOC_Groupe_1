import unittest
from unittest.mock import MagicMock, patch
from mongo_to_mysql import recur_message, traitement

class TestMain(unittest.TestCase):

    def test_recur_message(self):
        # Test recursive message function
        f = MagicMock()
        msg = {'id': 1, 'created_at': '2022-01-01 12:00:00'}
        thread_id = 2
        recur_message(msg, f, thread_id)
        f.assert_called_once_with(msg, thread_id, None)

    def test_traitement(self):
        # Test message treatment function
        msg = {
            'id': 1,
            'type': 'post',
            'created_at': '2022-01-01 12:00:00',
            'user_id': 1,
            'depth': 0,
            'body': 'Hello World',
            'anonymous': False,
            'anonymous_to_peers': False,
        }
        parent_id = 2
        thread_id = 3
        stmts = {'Users_2': [], 'Messages': []}
        traitement(msg, thread_id, parent_id)
        self.assertEqual(len(stmts['Users_2']), 1)
        self.assertEqual(len(stmts['Messages']), 1)

if __name__ == '__main__':
    unittest.main()
