// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.security.Principal;
import java.util.Arrays;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Preface implements LogInfo {
    protected final String uri;
    protected final String method;
    protected final String user;
    protected final MultivaluedMap<String, String> headers;

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
        if (headers.put("Cookie", Arrays.asList("Cookies present in header but not logged")) == null) {
            headers.put("Cookie", Arrays.asList("Cookies not present in header"));
        }
    }
}
