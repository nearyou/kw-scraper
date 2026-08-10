"""Small, synchronous queue processor with retries and dead-letter handling."""

import logging
import time


class ReliableQueueHandler:
    """Process one message at a time without allowing failures to stop the queue."""

    def __init__(
        self,
        process_message,
        acknowledge_message,
        dead_letter_message,
        retry_delays=(1, 2, 4),
        sleep=time.sleep,
        logger=None,
    ):
        self.process_message = process_message
        self.acknowledge_message = acknowledge_message
        self.dead_letter_message = dead_letter_message
        self.retry_delays = tuple(retry_delays)
        self.sleep = sleep
        self.logger = logger or logging.getLogger(__name__)

    def handle(self, message):
        """Handle one message and return True only after it is acknowledged.

        The first attempt runs immediately. Each failure is followed by one of
        the configured delays (1, 2, then 4 seconds) before a retry. This gives
        a message three retries, or four total attempts. After the last attempt
        fails, the message is sent to the dead-letter queue. Exceptions are
        contained here so the caller can safely continue with the next message.
        """
        message_id = getattr(message, "id", repr(message))
        total_attempts = len(self.retry_delays) + 1
        last_error = None

        for attempt in range(1, total_attempts + 1):
            try:
                self.logger.info(
                    "Processing message %s (attempt %s/%s)",
                    message_id,
                    attempt,
                    total_attempts,
                )
                result = self.process_message(message)
                if result is False:
                    raise RuntimeError("message processor reported failure")

                # A message is not considered complete until success is confirmed.
                self.acknowledge_message(message)
                self.logger.info("Message %s processed and acknowledged", message_id)
                return True
            except Exception as error:
                last_error = error
                self.logger.exception(
                    "Message %s failed on attempt %s/%s",
                    message_id,
                    attempt,
                    total_attempts,
                )

                if attempt <= len(self.retry_delays):
                    delay = self.retry_delays[attempt - 1]
                    self.logger.warning(
                        "Retrying message %s in %s second(s)", message_id, delay
                    )
                    self.sleep(delay)

        try:
            self.dead_letter_message(message, last_error)
            self.logger.error(
                "Message %s moved to the dead-letter queue after %s attempts",
                message_id,
                total_attempts,
            )
        except Exception:
            # Dead-letter storage errors are logged but never crash the queue loop.
            self.logger.exception(
                "Could not move message %s to the dead-letter queue", message_id
            )
        return False
