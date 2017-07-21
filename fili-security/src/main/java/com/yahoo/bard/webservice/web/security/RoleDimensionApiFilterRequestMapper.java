// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import static com.yahoo.bard.webservice.web.security.SecurityErrorMessageFormat.DIMENSION_MISSING_MANDATORY_ROLE;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * A request mapper that ensures that a user has at least one relevant role on a dimension and applies access filters
 * based on roles for that user.
 * This mapper is intended to route based on user-category style roles such as routing super-user access to a
 * different mapping chain.
 */
public class RoleDimensionApiFilterRequestMapper extends ChainingRequestMapper<DataApiRequest> {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinderFactory.class);

    public static final String SECURITY_SIGNUP_MESSAGE_KEY = "security_signup_message";
    public static final String DEFAULT_SECURITY_MESSAGE = "Your security settings do not permit access to this " +
            "resource.  Please contact your user administration for access.";

    private final static String UNAUTHORIZED_USER_MESSAGE = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName(SECURITY_SIGNUP_MESSAGE_KEY),
            DEFAULT_SECURITY_MESSAGE
    );

    private final String unauthorizedHttpMessage;

    private Dimension dimension;
    private Map<String, Set<ApiFilter>> roleApiFilters;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param dimension  The dimension whose roles are being matched
     * @param roleApiFilters  ApiFilters by role for a given dimension
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleDimensionApiFilterRequestMapper(
            final ResourceDictionaries resourceDictionaries,
            Dimension dimension,
            Map<String, Set<ApiFilter>> roleApiFilters,
            RequestMapper<DataApiRequest> next
    ) {
        super(resourceDictionaries, next);
        this.dimension = dimension;
        this.roleApiFilters = roleApiFilters;
        unauthorizedHttpMessage = DIMENSION_MISSING_MANDATORY_ROLE.format(dimension.getApiName());
    }

    @Override
    protected DataApiRequest internalApply(DataApiRequest request, ContainerRequestContext context)
            throws RequestValidationException {
        SecurityContext securityContext = context.getSecurityContext();

        Set<ApiFilter> securityFilters = buildSecurityFilters(securityContext);

        validateSecurityFilters(securityContext.getUserPrincipal(), securityFilters);

        Map<Dimension, Set<ApiFilter>> revisedFilters = mergeSecurityFilters(request.getFilters(), securityFilters);

        return request.withFilters(revisedFilters);
    }

    /**
     * Merge the request filters with the dimension filters for this request.
     *
     * @param requestFilters  The set of all ApiFilters from a request
     * @param securityFilters  The filters produced by merging role-based filters
     *
     * @return  A set of request filters supplemented with filters from this request.
     */
    protected Map<Dimension, Set<ApiFilter>> mergeSecurityFilters(
            Map<Dimension, Set<ApiFilter>> requestFilters,
            Set<ApiFilter> securityFilters
    ) {
        Map<Dimension, Set<ApiFilter>> revisedFilters = new LinkedHashMap<>(requestFilters);
        Set<ApiFilter> requestDimensionFilters = revisedFilters.getOrDefault(
                dimension,
                Collections.emptySet()
        );
        // Merge the request filters (if any) with the security filters for this dimension
        revisedFilters.put(
                dimension,
                unionMergeFilterValues(Stream.concat(requestDimensionFilters.stream(), securityFilters.stream()))
        );
        return revisedFilters;
    }

    /**
     * Verify that, given this user, that at least some of the whitelisted filters have been collected.
     * Failure to have any whitelisted filters indicate a user has not been authorized for values with this dimension.
     *
     * @param userPrincipal  The userPrincipal being validated
     * @param mergedSecurityFilters  The combined security filters for this request
     *
     * @throws RequestValidationException  An http request exception documenting the lack of privileges
     */
    protected void validateSecurityFilters(Principal userPrincipal, Set<ApiFilter> mergedSecurityFilters)
            throws RequestValidationException {
        if (mergedSecurityFilters.isEmpty()) {
            LOG.warn(DIMENSION_MISSING_MANDATORY_ROLE.logFormat(userPrincipal.getName(), dimension.getApiName()));
            throw new RequestValidationException(
                    Response.Status.FORBIDDEN,
                    unauthorizedHttpMessage,
                    UNAUTHORIZED_USER_MESSAGE
            );
        }
    }

    /**
     * Collect all the whitelist filters generated by roles in this security context.
     *
     * @param securityContext  The security context of the request.
     *
     * @return  A set of filters whitelisting access to this dimension.
     */
    protected Set<ApiFilter> buildSecurityFilters(final SecurityContext securityContext) {
        return unionMergeFilterValues(
                    roleApiFilters.keySet().stream()
                            .filter(securityContext::isUserInRole)
                            .map(roleApiFilters::get)
                            .flatMap(Set::stream)
            );
    }

    /**
     * For a set of ApiFilters collect by dimension, field and operation, and union their value sets.
     *
     * @param filterStream  Stream of ApiFilters whose values are to be unioned
     *
     * @return A set of filters with the same operations but values unioned
     */
    protected static Set<ApiFilter> unionMergeFilterValues(Stream<ApiFilter> filterStream) {

        Function<ApiFilter, Triple<Dimension, DimensionField, FilterOperation>> filterGroupingIdentity = filter ->
            new ImmutableTriple<>(filter.getDimension(), filter.getDimensionField(), filter.getOperation());

        Map<Triple<Dimension, DimensionField, FilterOperation>, Set<String>> filterMap =
                filterStream.collect(Collectors.groupingBy(
                        filterGroupingIdentity::apply,
                        Collectors.mapping(
                                ApiFilter::getValues,
                                Collectors.reducing(Collections.emptySet(), StreamUtils::setMerge)
                        )
                ));

        return filterMap.entrySet().stream()
                .map(it -> new ApiFilter(
                        it.getKey().getLeft(),
                        it.getKey().getMiddle(),
                        it.getKey().getRight(),
                        it.getValue()
                ))
                .collect(Collectors.toSet());
    }
}
