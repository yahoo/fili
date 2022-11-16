// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import java.util.Map;

/**
 * A checked exception for errors emitted when protocol values cannot be processed.
 */
public class UnknownProtocolValueException extends RuntimeException {

    public static final String MESSAGE_FORMAT = "Unknown value for parameter %s: %s";

    private final Protocol protocol;
    private final Map<String, String> parameterValues;

    /**
     * Constructor.
     *
     * @param protocol  The protocol being processed.
     * @param parameterValues  The values for the protocol.
     */
    public UnknownProtocolValueException(Protocol protocol, Map<String, String> parameterValues) {
        super(String.format(
                MESSAGE_FORMAT,
                protocol.getCoreParameterName(),
                parameterValues.get(protocol.getCoreParameterName())
        ));
        this.protocol = protocol;
        this.parameterValues = parameterValues;
    }

    /**
     * The Protocol being checked.
     *
     * @return A protocol.
     */
    public Protocol getParameterName() {
        return protocol;
    }

    /**
     * The values applied for this protocol.
     *
     * @return A map of values.
     */
    public Map<String, String> getParameterValues() {
        return parameterValues;
    }
}
