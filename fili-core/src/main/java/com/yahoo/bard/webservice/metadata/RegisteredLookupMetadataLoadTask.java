// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.application.LoadTask;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Lookup Load task sends requests to Druid coordinator and returns load statuses of lookup metadata in Druid.
 */
public class RegisteredLookupMetadataLoadTask extends LoadTask<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(RegisteredLookupMetadataLoadTask.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * A comma separated string of lookup tiers.
     * <p>
     * See http://druid.io/docs/latest/querying/lookups.html for details on "tier".
     */
    private static final String TIERS_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_registered_lookup_tiers");
    /**
     * Location of lookup statuses on Druid coordinator.
     */
    private static final String LOOKUP_QUERY_FORMAT = "/lookups/status/%s";
    /**
     * Time between 2 consecutive lookup loading call in milliseconds.
     */
    private static final String LOOKUP_NORMAL_CHECKING_PERIOD_KEY = SYSTEM_CONFIG.getPackageVariableName(
            "lookup_normal_checking_period"
    );
    /**
     * TODO - Wait on https://github.com/yahoo/fili/issues/619.
     */
    private static final String LOOKUP_ERROR_CHECKING_PERIOD_KEY = SYSTEM_CONFIG.getPackageVariableName(
            "lookup_error_checking_period"
    );
    /**
     * Parameter specifying the delay before the first run of {@link RegisteredLookupMetadataLoadTask}, in milliseconds.
     */
    private static final String INITIAL_LOOKUP_CHECKING_DELAY = SYSTEM_CONFIG.getPackageVariableName(
            "initial_lookup_checking_delay"
    );

    private final DruidWebService druidClient;
    private final DimensionDictionary dimensionDictionary;
    private final SuccessCallback successCallback;
    private final FailureCallback failureCallback;
    private final HttpErrorCallback errorCallback;
    private final Set<String> pendingLookups;

    private Set<String> lookupTiers;


    /**
     * Constructor.
     *
     * @param druidClient  The client to query Druid coordinator
     * @param dimensionDictionary  A {@link com.yahoo.bard.webservice.data.dimension.DimensionDictionary} that is used
     * to obtain a list of lookups in Fili.
     */
    public RegisteredLookupMetadataLoadTask(DruidWebService druidClient, DimensionDictionary dimensionDictionary) {
        super(
                RegisteredLookupMetadataLoadTask.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(INITIAL_LOOKUP_CHECKING_DELAY, 0),
                SYSTEM_CONFIG.getLongProperty(LOOKUP_NORMAL_CHECKING_PERIOD_KEY, TimeUnit.MINUTES.toMillis(1))
        );
        this.druidClient = druidClient;
        this.dimensionDictionary = dimensionDictionary;
        this.successCallback = buildLookupSuccessCallback();
        this.failureCallback = getFailureCallback();
        this.errorCallback = getErrorCallback();
        this.pendingLookups = new HashSet<>();
        this.lookupTiers = getTiers(SYSTEM_CONFIG.getStringProperty(TIERS_KEY, "__default"));
    }

    @Override
    public void run() {
        getPendingLookups().clear();
        // download load statuses of all lookups of each lookup tier
        lookupTiers.stream()
                .peek(tier -> LOG.trace("Querying metadata for lookup tier: {}", tier))
                .forEach(lookupTier -> {
                    druidClient.getJsonObject(
                            successCallback,
                            errorCallback,
                            failureCallback,
                            String.format(LOOKUP_QUERY_FORMAT, lookupTier)
                    );
                });
    }

    /**
     * Returns a list of configured lookup tiers.
     * <p>
     * See http://druid.io/docs/latest/querying/lookups.html for details on "tier".
     *
     * @return the list of configured lookup tiers
     */
    public Set<String> getLookupTiers() {
        return lookupTiers;
    }

    /**
     * Updates list of configured lookup tiers.
     * <p>
     * This method becomes useful when a new look up tier is added at runtime.
     *
     * @param lookupTiers  A new list of configured lookup tiers
     */
    public void setLookupTiers(Set<String> lookupTiers) {
        this.lookupTiers = lookupTiers;
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

            pendingLookups.addAll(
                    dimensionDictionary.findAll().stream()
                            .filter(dimension -> dimension instanceof RegisteredLookupDimension)
                            .map(dimension -> (RegisteredLookupDimension) dimension)
                            .map(RegisteredLookupDimension::getRegisteredLookupExtractionFns)
                            .flatMap(List::stream)
                            .map(extractionFunction ->
                                    ((RegisteredLookupExtractionFunction) extractionFunction).getLookup()
                            )
                            .peek(namespace -> LOG.trace("Checking lookup metadata status for {}", namespace))
                            .filter(lookup -> !lookupStatuses.containsKey(lookup) || !lookupStatuses.get(lookup))
                            .collect(Collectors.toSet())
            );
        };
    }

    /**
     * Returns a list of lookup tiers from a comma separated string.
     * <p>
     * For example, {@code "tier1,tier2,tier3"} becomes {@code ["tier1", "tier2", "tier3"]}.
     *
     * @param string  The comma separated string of lookup tiers.
     *
     * @return the list of lookup tiers
     */
    private static Set<String> getTiers(String string) {
        return Arrays.stream(string.split(",")).collect(Collectors.toSet());
    }
}
