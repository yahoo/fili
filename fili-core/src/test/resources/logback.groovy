// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
// See http://logback.qos.ch/manual/groovy.html

// Appenders
appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS}\t%p\t[%t]\t%mdc{logid:-unset}\t%c{36} - %m%n"
    }
}

// Loggers
logger("com.yahoo.bard", DEBUG)
root(INFO, ["STDOUT"])
