// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import java.util.LinkedHashMap;

/**
 * Protocol Dictionary collects Protocols for the system.
 *
 * The key should be the contract name for the Protocols supported.
 */
public class ProtocolDictionary extends LinkedHashMap<String, Protocol> {

    /**
     * Convenience method to add protocols to the dictionary.
     *
     * @param protocol  A protocol to add to the dictionary
     *
     * @return  The replaced protocol from the dictionary, if any.
     */
    public Protocol add(Protocol protocol) {
        return this.put(protocol.getContractName(), protocol);
    }
}
