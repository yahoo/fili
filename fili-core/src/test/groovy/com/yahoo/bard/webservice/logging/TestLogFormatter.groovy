// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
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
