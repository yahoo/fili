package com.yahoo.bard.webservice.logging

/**
 * A Test implementation of the `LogFormatter` class for the purpose of testing the LogFormatterProvider.
 */
class TestLogFormatter implements LogFormatter {
    @Override
    String format(LogBlock logBlock) {
        return "Hello world!"
    }
}
