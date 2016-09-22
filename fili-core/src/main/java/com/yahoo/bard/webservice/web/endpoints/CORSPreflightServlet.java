// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import javax.inject.Singleton;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.core.Response;

/**
 * Class to handle Preflighted requests.
 */
@Singleton
public class CORSPreflightServlet {

    /**
     * Responds to the OPTIONS preflight request.
     *
     * @return response with status 200
     */
    @OPTIONS
    public Response preflightResponse() {
        return Response.ok().build();
    }
}
