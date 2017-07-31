// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Collections;
import java.util.function.Supplier;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Logging block for the "inbound" request information, serving as a preface to the subsequent logging blocks.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Preface implements LogInfo {

    private static final Logger LOG = LoggerFactory.getLogger(Preface.class);

    public static final String NO_USER_PRINCIPAL_USER_NAME = "NO_USER_PRINCIPAL";

    protected final String uri;
    protected final String method;
    protected final MultivaluedMap<String, String> headers;

    @JsonIgnore
    protected final Supplier<String> user;

    /**
     * Calls get on the {@link #user} at serialization.
     *
     * @return the name of the user
     */
    @JsonProperty
    public String getUser() {
        return user.get();
    }

    /**
     * Constructor.
     *
     * @param request  Request to extract preface info from
     */
    public Preface(ContainerRequestContext request) {
        uri = request.getUriInfo().getRequestUri().toASCIIString();
        method = request.getMethod();

        user = () -> {
            Principal principal = request.getSecurityContext().getUserPrincipal();
            if (principal != null) {
                return principal.getName();
            } else {
                // No principal found
                LOG.warn("No user principal detected when building the Preface. This shouldn't happen!");
                return NO_USER_PRINCIPAL_USER_NAME;
            }
        };

        headers = request.getHeaders();
        //Discard all cookies from header by overriding the entry
        if (headers.put("Cookie", Collections.singletonList("Cookies present in header but not logged")) == null) {
            headers.put("Cookie", Collections.singletonList("Cookies not present in header"));
        }
    }
}
