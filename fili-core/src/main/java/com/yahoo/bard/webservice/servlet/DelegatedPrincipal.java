// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.servlet;

import java.security.Principal;
public interface DelegatedPrincipal extends Principal {

    /**
     * The user principal that originated the request that is now being fulfilled by this principal.
     *
     * @return the Principal that this Principal was creatured under.
     */
    Principal deletegatedFrom();
}
