import unittest
from unittest.mock import Mock

from www.queue_handler import ReliableQueueHandler


class Message:
    id = 42


class ReliableQueueHandlerTests(unittest.TestCase):
    def make_handler(self, processor):
        self.acknowledge = Mock()
        self.dead_letter = Mock()
        self.sleep = Mock()
        self.logger = Mock()
        return ReliableQueueHandler(
            process_message=processor,
            acknowledge_message=self.acknowledge,
            dead_letter_message=self.dead_letter,
            sleep=self.sleep,
            logger=self.logger,
        )

    def test_acknowledges_successful_message(self):
        processor = Mock(return_value=True)
        handler = self.make_handler(processor)
        message = Message()

        self.assertTrue(handler.handle(message))
        processor.assert_called_once_with(message)
        self.acknowledge.assert_called_once_with(message)
        self.dead_letter.assert_not_called()
        self.sleep.assert_not_called()

    def test_retries_with_exponential_delays_then_acknowledges(self):
        processor = Mock(side_effect=[RuntimeError("one"), RuntimeError("two"), RuntimeError("three"), True])
        handler = self.make_handler(processor)
        message = Message()

        self.assertTrue(handler.handle(message))
        self.assertEqual(processor.call_count, 4)
        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [1, 2, 4])
        self.acknowledge.assert_called_once_with(message)
        self.dead_letter.assert_not_called()

    def test_moves_message_to_dead_letter_after_three_retries(self):
        error = RuntimeError("still broken")
        processor = Mock(side_effect=error)
        handler = self.make_handler(processor)
        message = Message()

        self.assertFalse(handler.handle(message))
        self.assertEqual(processor.call_count, 4)
        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [1, 2, 4])
        self.acknowledge.assert_not_called()
        self.dead_letter.assert_called_once_with(message, error)

    def test_dead_letter_failure_does_not_escape(self):
        handler = self.make_handler(Mock(side_effect=RuntimeError("broken")))
        self.dead_letter.side_effect = RuntimeError("dead-letter unavailable")

        self.assertFalse(handler.handle(Message()))

    def test_next_message_can_run_after_a_permanent_failure(self):
        processor = Mock(side_effect=RuntimeError("broken"))
        handler = self.make_handler(processor)

        self.assertFalse(handler.handle(Message()))
        processor.side_effect = None
        processor.return_value = True
        self.assertTrue(handler.handle(Message()))
        self.acknowledge.assert_called_once()


if __name__ == "__main__":
    unittest.main()
