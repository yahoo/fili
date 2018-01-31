// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.application.LoadTask;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Lookup Load task sends requests to Druid coordinator and returns list of configured lookup statuses in Druid.
 */
public class LookupMetadataLoadTask extends LoadTask<Boolean> {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * Location of lookup statuses on Druid coordinator.
     */
    public static final String LOOKUP_QUERY = "/lookups/status/__default";
    /**
     * Time between 2 consecutive lookup loading call in milliseconds.
     */
    public static final String LOOKUP_NORMAL_CHECKING_PERIOD_KEY = SYSTEM_CONFIG.getPackageVariableName(
            "lookup_normal_checking_period"
    );
    /**
     * Wait on https://github.com/yahoo/fili/issues/619.
     */
    public static final String LOOKUP_ERROR_CHECKING_PERIOD_KEY = SYSTEM_CONFIG.getPackageVariableName(
            "lookup_error_checking_period"
    );
    /**
     * Parameter specifying the delay before the first run of {@link LookupMetadataLoadTask}, in milliseconds.
     */
    public static final String INITIAL_LOOKUP_CHECKING_DELAY = SYSTEM_CONFIG.getPackageVariableName(
            "initial_lookup_checking_delay"
    );

    private final DruidWebService druidClient;
    private final DimensionDictionary dimensionDictionary;
    private final SuccessCallback successCallback;
    private final FailureCallback failureCallback;
    private final HttpErrorCallback errorCallback;
    private Set<String> pendingLookups;

    /**
     * Constructor.
     *
     * @param druidClient  The client to query Druid coordinator
     * @param dimensionDictionary  A {@link com.yahoo.bard.webservice.data.dimension.DimensionDictionary} that is used
     * to obtain a list of lookups in Fili.
     */
    public LookupMetadataLoadTask(DruidWebService druidClient, DimensionDictionary dimensionDictionary) {
        super(
                LookupMetadataLoadTask.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(INITIAL_LOOKUP_CHECKING_DELAY, 0),
                SYSTEM_CONFIG.getLongProperty(LOOKUP_NORMAL_CHECKING_PERIOD_KEY, TimeUnit.MINUTES.toMillis(1))
        );
        this.druidClient = druidClient;
        this.dimensionDictionary = dimensionDictionary;
        this.successCallback = buildLookupSuccessCallback();
        this.failureCallback = getFailureCallback();
        this.errorCallback = getErrorCallback();
    }

    @Override
    public void run() {
        // download load statuses of all lookups
        druidClient.getJsonObject(successCallback, errorCallback, failureCallback, LOOKUP_QUERY);
    }

    /**
     * Returns a set of lookup namespaces that have not been loaded to Druid yet.
     *
     * @return the set of lookup namespaces that have not been loaded to Druid yet
     */
    public Set<String> getPendingLookups() {
        return pendingLookups;
    }

    /**
     * Returns a callback that has actions on lookup metadata from a successful Druid response.
     * <p>
     * The callback obtains a complete list of configured lookups from Druid coordinator, compares this list against
     * the list of lookups configured in Fili, and finds all lookup namespace names that are either not loaded yet in
     * Druid or does not exist in Druid at all. These namespaces can be retrieved later by calling
     * {@link #getPendingLookups()}.
     *
     * @return the callback that has actions on lookups from a successful Druid response
     */
    protected SuccessCallback buildLookupSuccessCallback() {
        return rootNode -> {
            Map<String, Boolean> lookupStatuses = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> entries = rootNode.fields();
            while (entries.hasNext()) {
                Map.Entry<String, JsonNode> entry = entries.next();
                lookupStatuses.put(entry.getKey(), entry.getValue().get("loaded").asBoolean());
            }

            pendingLookups = dimensionDictionary.findAll().stream()
                    .filter(dimension -> dimension instanceof LookupDimension)
                    .map(dimension -> (LookupDimension) dimension)
                    .map(LookupDimension::getNamespaces)
                    .flatMap(List::stream)
                    .filter(namespace -> !lookupStatuses.containsKey(namespace) || !lookupStatuses.get(namespace))
                    .collect(Collectors.toSet());
        };
    }
}
