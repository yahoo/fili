package com.yahoo.bard.webservice.data.config.metric.parser;

/**
 * Unchecked exception thrown when parsing metric.
 */
public class MetricParseException extends RuntimeException {

    /**
     * Construct a new MetricParseException.
     *
     * @param message the exception message
     */
   public MetricParseException(String message) {
      super(message);
   }
}
