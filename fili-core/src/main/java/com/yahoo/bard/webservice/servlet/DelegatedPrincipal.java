package com.yahoo.bard.webservice.servlet;

import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;

public interface DelegatedPrincipal extends Principal {

    /**
     * The user principal that originated the request that is now being fulfilled by this principal.
     * @return
     */
    Principal deletegatedFrom();
}
