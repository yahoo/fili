package com.yahoo.bard.webservice.web;

import java.security.Principal;

public interface RateLimiter {

    RequestToken getToken(RequestType type, Principal user);
}
