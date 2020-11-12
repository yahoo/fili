// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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

    private static final String UNNAMED = "";

    /**
     * Contracts which should not be supported on this metric or metrics that depend on it.
     */
    private final Set<String> blacklist;

    /**
     * Protocols supported for this metric, keyed by contract name.
     */
    private final Map<String, Protocol> protocolMap;

    /**
     * Protocols supported for this metric, keyed by contract name.
     */
    private final Map<String, Protocol> protocolParameterMap;

    /**
     * Name of the ProtocolSupport instance. Name is optional and exists solely as a convenience for metadata
     */
    private final String name;

    /**
     * Constructor.
     *
     * @param protocols A collection of protocols to support.
     */
    public ProtocolSupport(
            Collection<Protocol> protocols
    ) {
        this(protocols, Collections.emptySet(), UNNAMED);
    }

    /**
     * Constructor. Name is defaulted to empty string, indicating that this protocol support will not publish metadata
     * for the base metric.
     *
     * @param protocols  A collection of protocols to support.
     * @param blacklist  Protocols that will not be supported and should not be supported by depending metrics.
     */
    public ProtocolSupport(
            Collection<Protocol> protocols,
            Set<String> blacklist
    ) {
        this(protocols, blacklist, UNNAMED);
    }

    /**
     * Constructor.
     *
     * @param protocols  A collection of protocols to support.
     * @param blacklist  Protocols that will not be supported and should not be supported by depending metrics.
     * @param name  The name of this ProtocolSupport. Name is a metadata and organizational concept, it is not used
     *              internally to identify ProtocolSupport instances.
     */
    public ProtocolSupport(
            Collection<Protocol> protocols,
            Set<String> blacklist,
            String name
    ) {
        protocolMap = protocols.stream().collect(Collectors.toMap(Protocol::getContractName, Function.identity()));
        protocolParameterMap = protocols.stream()
                .collect(Collectors.toMap(Protocol::getCoreParameterName, Function.identity()));
        this.blacklist = blacklist;
        this.name = name;
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
     * Determine if this protocol is supported.
     *
     * @param parameterName The core parameter for a protocol.
     *
     * @return true if this protocol is not blacklisted and supplied by protocol map.
     */
    public boolean acceptsParameter(String parameterName) {
        Protocol protocol = protocolParameterMap.get(parameterName);
        return  protocol != null && ! blacklist.contains(protocol.getContractName());
    }

    /**
     * Return a set of supported protocol names.
     *
     * This allows easy classification, and identification of supported contracts.
     *
     * @return Sorted names of the supported protocols
     */
    public SortedSet<String> getProtocolNames() {
        TreeSet<String> protocolNames = new TreeSet<>(protocolMap.keySet());
        protocolNames.removeAll(blacklist);
        return protocolNames;
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
     * Create a copy which supports additional protocols.
     *
     * @param addingProtocols  Additional protocols to support.
     *
     * @return A protocol support which accepts these protocols.
     */
    public ProtocolSupport withReplaceProtocols(Collection<Protocol> addingProtocols) {
        // Add any addedProtocols to the map, replacing if contract name conflicts
        Map<String, Protocol> newProtocols = new HashMap<>(this.protocolParameterMap);
        addingProtocols.stream()
                .forEach(protocol -> newProtocols.put(protocol.getCoreParameterName(), protocol));

        // Remove any added protocols from the blacklist
        Set<String> newBlackList = new HashSet<String>(blacklist);
        newProtocols.values().stream()
                .map(Protocol::getContractName)
                .forEach(name -> newBlackList.remove(name));

        return new ProtocolSupport(newProtocols.values(), newBlackList);
    }

    /**
     * Retrieve the protocol value for a given protocol contract name.
     *
     * @param protocolName a protocol contract name
     *
     * @return The Protocol for this protocol name
     */
    public Protocol getProtocol(String protocolName) {
        return protocolMap.get(protocolName);
    }

    /**
     * Returns this name of this Protocol support. Name is simply a convenience for exposing ProtocolSupports in
     * metadata or for clients to track ProtocolSupports. Names are optional and have no format restrictions.
     *
     * @return the name of the ProtocolSupport or empty string if the ProtocolSupport is unnamed.
     */
    public String getName() {
        return name;
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
