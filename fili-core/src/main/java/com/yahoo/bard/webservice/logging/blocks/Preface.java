// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.security.Principal;
import java.util.Collections;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Logging block for the "inbound" request information, serving as a preface to the subsequent logging blocks.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Preface implements LogInfo {
    protected final String uri;
    protected final String method;
    protected final String user;
    protected final MultivaluedMap<String, String> headers;

    /**
     * Constructor.
     *
     * @param request  Request to extract preface info from
     */
    public Preface(ContainerRequestContext request) {
        uri = request.getUriInfo().getRequestUri().toASCIIString();
        method = request.getMethod();
        Principal principal = request.getSecurityContext().getUserPrincipal();
        if (principal != null) {
            user = principal.getName();
        } else {
            // No principal found
            user = "";
        }

        headers = request.getHeaders();
        //Discard all cookies from header by overriding the entry
        if (headers.put("Cookie", Collections.singletonList("Cookies present in header but not logged")) == null) {
            headers.put("Cookie", Collections.singletonList("Cookies not present in header"));
        }
    }
}
