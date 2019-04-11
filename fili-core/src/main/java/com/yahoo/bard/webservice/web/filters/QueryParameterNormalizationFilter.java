// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Singleton;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * Normalize all query parameter keys to lowercase.
 */
@Singleton
@PreMatching
public class QueryParameterNormalizationFilter implements ApplicationEventListener, ContainerRequestFilter {
    private static final Logger LOG = LoggerFactory.getLogger(QueryParameterNormalizationFilter.class);

    private static Map<String, String> parameterMap = null;

    @Override
    public void onEvent(ApplicationEvent applicationEvent) {
        if (parameterMap == null && applicationEvent.getType() == ApplicationEvent.Type.INITIALIZATION_START) {
            Set<Class<?>> providers = applicationEvent.getProviders();
            ClassLoader classLoader = applicationEvent.getResourceConfig().getClassLoader();
            parameterMap = buildParameterMap(providers, classLoader);
        }
    }

    @Override
    public RequestEventListener onRequest(RequestEvent requestEvent) {
        // Do nothing per request
        return null;
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        if (parameterMap == null || parameterMap.isEmpty()) {
            return; // Disable filter if this occurs
        }
        URI normalizedUri = buildNormalizedUri(containerRequestContext.getUriInfo());
        containerRequestContext.setRequestUri(normalizedUri);
    }

    /**
     * Build a normalized URI containing all LOWERCASE parameter keys.
     * <p>
     * NOTE: Value casing is unaffected.
     *
     * @param uriInfo Uri info object from container request context
     * @return  Normalized URI
     */
    private URI buildNormalizedUri(UriInfo uriInfo) {
        UriBuilder builder = uriInfo.getRequestUriBuilder();

        // Erase existing query parameters from builder
        builder.replaceQuery("");

        // Set normalized values
        getNormalizedQueryParameters(uriInfo)
                .forEach(keyValue -> builder.queryParam(keyValue.getLeft(), keyValue.getRight()));

        return builder.build();
    }

    /**
     * Extract a list of parameters and normalize all keys to be lowercase.
     * <p>
     * NOTE: To preserve existing functionality, I won't change the value of any endpoint parameter name
     *       (i.e. the endpoint can remain case sensitive in absence of this filter). As a result, we have
     *       a naive solution here that applies a one-to-one mapping for changing the casing appropriately.
     *       Everything else is lowercased. A potentially "better" solution would be to make all endpoint
     *       parameters <em>lowercase</em> while simply converting all parameter names to lowercase values.
     *
     * @param uriInfo UriInfo containing parameters to be normalized
     * @return  List of key-value pairs with normalized keys
     */
    private Stream<Pair<String, String>> getNormalizedQueryParameters(UriInfo uriInfo) {
        return uriInfo.getQueryParameters().entrySet().stream()
                .flatMap(entry -> {
                    String key = entry.getKey().toLowerCase(Locale.ENGLISH);
                    return entry.getValue().stream().map(value -> Pair.of(parameterMap.getOrDefault(key, key), value));
                });
    }

    /**
     * Build a parameter map that searches for @QueryParam annotations on the jersey endpoints
     * and extract their names.
     * This map enables us to perform case insensitive translations.
     * <p>
     * Detail:
     *   This method extracts all classes contained within the packages specified as "jersey provider packages."
     *   It then conditions these methods by enumerating all methods and keeping only those that are annotated
     *   as JAX-RS endpoints (+ the bard @PATCH annotation). After harvesting this list of method, it then enumerates
     *   all of the parameters for each method and keeps a list of the @QueryParam values. It then extracts the values
     *   from these @QueryParam annotations to retain its codified casing while also constructing a map of lowercase'd
     *   values to map in a case insensitive way.
     * <p>
     * NOTE: The ClassLoader provided with the ResourceConfig is used to find classes.
     *
     * @param providers Set of provider classes to seek for @QueryParam
     * @param classLoader Class loader to use while searching for specified packages
     * @return  Parameter map containing all of the case insensitive to case sensitive @QueryParam values
     */
    private static Map<String, String> buildParameterMap(Set<Class<?>> providers, ClassLoader classLoader) {
        if (providers == null) {
            LOG.warn("No providers defined. Disabling QueryParameterNormalizationFilter.");
            return Collections.emptyMap();
        } else if (classLoader == null) {
            LOG.warn("No valid ClassLoader found from context. Disabling QueryParameterNormalizationFilter.");
            return Collections.emptyMap();
        }
        return providers.stream()
                // Extract all of the corresponding methods from these classes
                .flatMap(QueryParameterNormalizationFilter::extractMethods)
                // Determine which methods are annotated as a JAX-RS endpoint
                .filter(QueryParameterNormalizationFilter::isWebEndpoint)
                // For each of these methods, extract the @QueryParam annotations from the parameter list
                .flatMap(QueryParameterNormalizationFilter::extractQueryParameters)
                // Extract the parameter value
                .map(QueryParam::value)
                .distinct()
                // Map the lower-case'd parameter value to its @QueryParam case'd counterpart
                .collect(
                        Collectors.toMap(
                                param -> param.toLowerCase(Locale.ENGLISH),
                                Function.identity(),
                                QueryParameterNormalizationFilter::resolveMapConflicts
                        )
                );
    }

    /**
     * Determine whether or not a method is annotated as a JAX-RS endpoint.
     *
     * @param method  Method to check
     * @return True if annotated as JAX-RS endpoint, false otherwise
     */
    private static boolean isWebEndpoint(Method method) {
        return Stream.of(method.getAnnotations())
                .anyMatch(annotation -> annotation.annotationType().isAnnotationPresent(HttpMethod.class));
    }

    /**
     * Extract a set of methods from a given class.
     *
     * @param clazz Class to extract methods from
     * @return  Stream of methods on class
     */
    private static Stream<Method> extractMethods(Class clazz) {
        try {
            Method[] methods = clazz.getMethods();
            if (methods.length > 0) {
                return Stream.of(methods);
            }
        } catch (Exception | Error e) {
            LOG.warn("Problems loading class at startup: {}", clazz, e);
        }
        return Stream.empty();
    }

    /**
     * Extract query parameters from a stream of methods.
     *
     * @param method Method to extract from which to extract @QueryParam's
     * @return  A stream of QueryParam annotations
     */
    private static Stream<QueryParam> extractQueryParameters(Method method) {
        return Stream.of(method.getParameterAnnotations())
                .flatMap(Stream::of)
                .filter(QueryParam.class::isInstance)
                .map(QueryParam.class::cast);
    }

    /**
     * Merge function for combining stream elements to a map.
     * If two or more strings map and are not identical, then log a warning and throw an exception.
     *
     * @param left  Left-hand string
     * @param right Right-hand string
     * @return  If left and right are equal, then return left. Otherwise throw a RuntimeException.
     */
    private static String resolveMapConflicts(String left, String right) {
        if (!left.equals(right)) {
            LOG.error("Two different casing's detect for parameter: '{}' ('{}' and '{}' found).\n" +
                    "    All casing for identical query parameters must be identical. Cannot proceed.",
                    left.toLowerCase(Locale.ENGLISH), left, right);
            throw new RuntimeException("Found different parameter casing-styles for parameter: " + left);
        }
        return left;
    }
}
