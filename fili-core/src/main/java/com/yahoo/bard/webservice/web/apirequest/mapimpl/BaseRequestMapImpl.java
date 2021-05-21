// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class BaseRequestMapImpl {

    protected static final Logger LOG = LoggerFactory.getLogger(BaseRequestMapImpl.class);
    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    protected static final Map<String, String> DEFAULT_PARAMS = new HashMap<>();
    protected static final Map<String, Object> DEFAULT_RESOURCES = new HashMap<>();
    protected static final Map<String, Object> DEFAULT_BINDERS = new HashMap<>();

    protected final Map<String, String> requestParams;
    protected final Map<String, Object> resources;
    protected final Map<String, Object> binders;
    protected final Map<String, Object> boundObjectCache;

    /**
     * Add default default parameter values.
     *
     * @param newDefaults  default values to add.
     */
    public static void addDefaultDefaultParamValues(Map<String, String> newDefaults) {
        DEFAULT_PARAMS.putAll(newDefaults);
    }

    /**
     * Add default default parameter values.
     *
     * @param toRemove  default values to remove.
     */
    public static void removeDefaultDefaultParamValues(Collection<String> toRemove) {
        toRemove.forEach(DEFAULT_PARAMS::remove);
    }

    /**
     * Add default default resource.
     *
     * @param key The property name for the resource
     * @param resource  The resource being provided by default
     * @param <T> The type of the resource being provided
     *
     * @return Any existing defaulted resource
     */
    @SuppressWarnings("unchecked")
    public static <T> T putDefaultResource(String key, T resource) {
        return (T) DEFAULT_BINDERS.put(key, resource);
    }

    /**
     * Add default default binder.
     *
     * @param key The property name for the binder
     * @param resource  The binder being provided by default
     * @param <T> The type of the binder being provided
     *
     * @return Any existing defaulted resource
     */
    @SuppressWarnings("unchecked")
    public static <T> T putDefaultBinders(String key, T resource) {
        return (T) DEFAULT_BINDERS.put(key, resource);
    }

    /**
     * Constructor.
     *
     * Constructor used to build ApiRequest objects.
     *
     * @param requestParams The map describing the parameters.
     * @param resources Non parameter request bound objects.
     */
    public BaseRequestMapImpl(
            Map<String, String> requestParams,
            Map<String, Object> resources
    ) {
        this.requestParams = new HashMap<>(Collections.unmodifiableMap(requestParams));
        this.resources = new HashMap<>(Collections.unmodifiableMap(resources));
        binders = new HashMap<>(Collections.unmodifiableMap(getDefaultBinders()));
        boundObjectCache = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    protected Optional<?> bindAndGetOptionalProperty(String key) {
        if (! boundObjectCache.containsKey(key)) {
            Optional<String> value = Optional.ofNullable(requestParams.getOrDefault(key, getDefaultParams().get(key)));
            Function<String, ?> binder = (Function<String, ?>) binders.get(key);
            boundObjectCache.put(key, value.map(binder));
        }
        return (Optional<?>) boundObjectCache.get(key);
    }

    @SuppressWarnings("unchecked")
    protected Object bindAndGetNonOptionalProperty(String key) {
        if (!boundObjectCache.containsKey(key)) {
            String value = requestParams.getOrDefault(key, getDefaultParams().get(key));
            Function<String, ?> binder = (Function<String, ?>) binders.get(key);
            boundObjectCache.put(key, binder.apply(value));
        }
        return boundObjectCache.get(key);
    }

    /**
     * Add a request parameter.
     *
     * @param key  The key name for the request parameter.
     * @param value The value of the request parameter.
     */
    public void setRequestParameter(String key, String value) {
        requestParams.put(key, value);
    }

    /**
     * Add an object containing request information.
     *
     * @param key  The key for the request object.
     * @param value  The value of the request object.
     * @param <T>   The type of the binder
     *
     * @return The resource being replaced
     */
    @SuppressWarnings("unchecked")
    public <T> T putResource(String key, T value) {
        return (T) resources.put(key, value);
    }

    /**
     * Add a factory for processing request objects and parameters.
     *
     * @param key  The key for the binder.
     * @param value  The binder instance.
     * @param <T>   The type of the binder
     *
     * @return The binder being replaced
     */
    @SuppressWarnings("unchecked")
    public <T> T putBinder(String key, T value) {
        return (T) binders.put(key, value);
    }

    /**
     * Get a request parameter.
     *
     * @param key  The key for the parameter.
     *
     * @return  A string describing a request parameter.
     */
    public String getRequestParameter(String key) {
        return requestParams.get(key);
    }

    /**
     * Get a request object.
     *
     * @param key The key for the request object.
     *
     * @return The instance of the request object.
     */
    public Object getResource(String key) {
        return resources.get(key);
    }

    /**
     * Get the binder for a particular property.
     *
     * @param key  The name of the property to build.
     *
     * @return The factory for that property.
     */
    public Object getBinder(String key) {
        return binders.get(key);
    }

    /**
     * Getter.
     *
     * Non static view of defaults allowing class level overrides.
     *
     * @return A map of default parameters
     */
    public Map<String, String> getDefaultParams() {
        return DEFAULT_PARAMS;
    }

    /**
     * Getter.
     *
     * Non static view of defaults allowing class level overrides.
     *
     * @return A map of default resources
     */
    public Map<String, Object> getDefaultResources() {
        return DEFAULT_RESOURCES;
    }

    /**
     * Getter.
     *
     * Non static view of defaults allowing class level overrides.
     *
     * @return A map of default binders
     */
    public Map<String, Object> getDefaultBinders() {
        return DEFAULT_BINDERS;
    }
}
