// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Protocol supports define which protocols a metric can support and to supply those supported protocols.
 *
 * A Protocol support has a map of protocols keyed by the contract name of the protocol.  It also has a blacklist
 * which defines protocols which are explicitly not supported for metrics which depend on this metric.
 */
public class ProtocolSupport {

    /**
     * Contracts which should not be supported on this metric or metrics that depend on it.
     */
    private final Set<String> blacklist;

    /**
     * Protocols supported for this metric, keyed by contract name.
     */
    private final Map<String, Protocol> protocolMap;

    /**
     * Constructor.
     *
     * @param protocols A collection of protocols to support.
     */
    public ProtocolSupport(
            Collection<Protocol> protocols
    ) {
        this(protocols, Collections.emptySet());
    }

    /**
     * Constructor.
     *
     * @param protocols  A collection of protocols to support.
     * @param blacklist  Protocols that will not be supported and should not be supported by depending metrics.
     */
    public ProtocolSupport(
            Collection<Protocol> protocols,
            Set<String> blacklist
    ) {
        protocolMap = protocols.stream().collect(Collectors.toMap(Protocol::getContractName, Function.identity()));
        this.blacklist = blacklist;
    }

    /**
     * Determine if this protocol is supported.
     *
     * @param protocolName The name of the protocol.
     *
     * @return true if this protocol is not blacklisted and supplied by protocol map.
     */
    public boolean accepts(String protocolName) {
        return ! blacklist.contains(protocolName) && protocolMap.containsKey(protocolName);
    }

    /**
     * Determine if this protocol is blacklisted.
     *
     * @param protocolName The name of the protocol.
     *
     * @return true if this protocol is blacklisted
     */
    public boolean isBlacklisted(String protocolName) {
        return blacklist.contains(protocolName);
    }

    /**
     * Create a modified copy with this protocol blacklisted.
     *
     * @param protocolName  The name of a protocol to not support.
     *
     * @return A protocol support with a protocol blacklisted.
     */
    public ProtocolSupport blacklistProtocol(String protocolName) {
        return blackListProtocols(Collections.singleton(protocolName));
    }

    /**
     * Create a modified copy with these protocols blacklisted.
     *
     * @param protocolNames  The names of a protocol to not support.
     *
     * @return A protocol support with protocols blacklisted.
     */
    public ProtocolSupport blackListProtocols(Collection<String> protocolNames) {
        // Remove all the protocols from the protocol map
        List<Protocol> protocols =
                protocolMap.values().stream()
                        .filter(protocol -> !protocolNames.contains(protocol.getContractName()))
                        .collect(Collectors.toList());

        // Add all the blacklisted names to the blacklist
        Set<String> newBlackList = Stream.concat(protocolNames.stream(), blacklist.stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return new ProtocolSupport(protocols, newBlackList);
    }

    /**
     * Create a copy combines the blacklist of other protocols.
     *
     * @param protocolSupports  Protocol supports whose blacklists should not be combined.
     *
     * @return A protocol support with additional protocols not supported.
     */
    public ProtocolSupport mergeBlacklists(Collection<ProtocolSupport> protocolSupports) {
        // Union the blacklists of several protocols
        List<String> protocols =
                protocolSupports.stream()
                        .flatMap(support -> support.blacklist.stream())
                        .collect(Collectors.toList());
        return blackListProtocols(protocols);
    }

    /**
     * Create a copy which supports additional protocols.
     *
     * @param addingProtocols  Additional protocols to support.
     *
     * @return A protocol support which accepts these protocols.
     */
    public ProtocolSupport withProtocols(Collection<Protocol> addingProtocols) {
        // Add any addedProtocols to the map
        Collection<Protocol> newProtocols = Stream.concat(addingProtocols.stream(), protocolMap.values().stream())
                .collect(Collectors.toSet());

        // Remove any added protocols from the blacklist
        Set<String> newBlackList = new HashSet<String>(blacklist);
        addingProtocols.stream()
                .map(Protocol::getContractName)
                .forEach(name -> newBlackList.remove(name));

        return new ProtocolSupport(newProtocols, newBlackList);
    }

    /**
     * Retrieve the protocol value for a given protocol contract name.
     *
     * @param protocolName a protocol contract name
     *
     * @return The Protocol for this protocol name
     */
    Protocol getProtocol(String protocolName) {
        return protocolMap.get(protocolName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final ProtocolSupport that = (ProtocolSupport) o;
        // MetricTransformers won't generally be comparable and may be anonymous functions, so equality on them
        // is impractical to guarantee.  So instead simply use the protocol names as the basis for equality
        return Objects.equals(blacklist, that.blacklist) &&
                Objects.equals(protocolMap.keySet(), that.protocolMap.keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(blacklist, protocolMap.keySet());
    }

    @Override
    public String toString() {
        return "ProtocolSupport{" +
                "blacklist=" + blacklist +
                ", protocolMap=" + protocolMap +
                '}';
    }
}
