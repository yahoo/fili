// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.protocol.protocols.ReaggregationProtocol;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Source for default protocols and default protocol dictionary.
 */
public class DefaultSystemMetricProtocols {

    private static final ProtocolDictionary DEFAULT_PROTOCOL_DICTIONARY = new ProtocolDictionary();

    /**
     * The names for standard protocol contracts supplied to makers by default.
     *
     * Intentionally mutable to be managed at config time before building makers.
     */
    private static final Set<String> STANDARD_PROTOCOLS = new HashSet<>();

    /**
     * Add a protocol to the global protocol dictionary and as a default protocol.
     *
     * @param protocol  a new protocol to be supported globally
     *
     */
    public static void addAsStandardProtocol(Protocol protocol) {
        STANDARD_PROTOCOLS.add(protocol.getContractName());
        addProtocol(protocol);
    }

    /**
     * Remove a protocol contract from the standard protocol list.
     *
     * @param contractName  the name of the protocol contract to be removed.
     *
     * @return true if this contract was previously supported
     */
    public static boolean removeFromStandardProtocols(String contractName) {
        return STANDARD_PROTOCOLS.remove(contractName);
    }

    /**
     * Add a protocol to the global protocol dictionary and as a default protocol.
     *
     * @param protocol  a new protocol to be supported globally
     *
     */
    public static void addProtocol(Protocol protocol) {
        DEFAULT_PROTOCOL_DICTIONARY.put(protocol.getContractName(), protocol);
    }

    static {
        addAsStandardProtocol(ReaggregationProtocol.INSTANCE);
    }

    /**
     * Build the default protocol support for Protocol Metric Makers.
     *
     * @return  A Protocol Support describing the default protocols supported throughout the system.
     */
    public static ProtocolSupport getStandardProtocolSupport() {
        return new ProtocolSupport(STANDARD_PROTOCOLS.stream()
                .map(DEFAULT_PROTOCOL_DICTIONARY::get)
                .collect(Collectors.toList()));
    }


    public static ProtocolDictionary getDefaultProtocolDictionary() {
        return DEFAULT_PROTOCOL_DICTIONARY;
    }

    /**
     * Constructor.
     *
     * Private to prevent instance creation.
     */
    private DefaultSystemMetricProtocols() {
    }
}
