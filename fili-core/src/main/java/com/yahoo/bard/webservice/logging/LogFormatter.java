// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

/**
 * Interface for log formatter that are meant to facilitate log exploration and data extraction.
 */
@FunctionalInterface
public interface LogFormatter {

    /**
     * Takes in a log block and returns a formatted String to be logged.
     *
     * @param logBlock  the log block that represents a log and that will be formatted in to the needed format.
     *
     * @return a formatted String to be logged
     */
    String format(LogBlock logBlock);
}
