// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.servlet

import static com.yahoo.bard.webservice.servlet.RoleImpersonationFilter.ROLE_NAME_ALIAS_KEY

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import spock.lang.Specification

import java.security.Principal
import java.util.function.Function
import java.util.function.Predicate

import javax.ws.rs.WebApplicationException
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.Response
import javax.ws.rs.core.SecurityContext

class RoleImpersonationFilterSpec extends Specification {

    Function<String, List<String>> roleFetcher = Mock(Function)

    Predicate<SecurityContext> isAuthorized = Mock(Predicate)

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    class TestImpersonationFilter extends RoleImpersonationFilter {

        public RoleImpersonationFilter() {
            super();
        }

        @Override
        List<String> getRolesForId(final String id) throws IOException {
            return roleFetcher.apply(id)
        }

        @Override
        boolean isAuthorizedToImpersonate(SecurityContext securityContext) throws IOException {
            return isAuthorized.test(securityContext)
        }

    }

    ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
    SecurityContext sc = Mock(SecurityContext)
    RoleImpersonationFilter raf
    MultivaluedMap map = new MultivaluedHashMap()
    Principal user = Mock(Principal)

    def setup() {
        sc.getUserPrincipal() >> user
        user.getName() >> "impersonatorUser"
        SYSTEM_CONFIG.setProperty(ROLE_NAME_ALIAS_KEY, "role2:role3");
        raf = new TestImpersonationFilter()
    }

    def "Role Impersonation filter checks right to impersonate and fetches roles"() {
        setup:
        String userName = "user1"
        String fromHeader = "user1@here.there"
        MultivaluedMap map = new MultivaluedHashMap()
        map.put("From", Collections.singletonList(fromHeader))
        List<String> roles = ["role1", "role3"]
        containerRequestContext.getHeaders() >> map
        containerRequestContext.getSecurityContext() >> sc
        SecurityContext newSc;
        containerRequestContext.setSecurityContext(_) >> {args -> newSc = args[0]}

        when:
        raf.filter(containerRequestContext)

        then:
        1 * isAuthorized.test(sc) >> true
        1 * roleFetcher.apply(userName) >> roles
        newSc.getUserPrincipal() instanceof DelegatedPrincipal
        ((DelegatedPrincipal) newSc.getUserPrincipal()).deletegatedFrom() == user
        newSc.isUserInRole("role1")
        newSc.isUserInRole("role2")
        newSc.getUserPrincipal().getName() == "user1"
    }

    def "Authorization error if unauthorized to impersonate"() {
        setup:
        String fromHeader = "user1@here.there"
        MultivaluedMap map = new MultivaluedHashMap()
        map.put("From", Collections.singletonList(fromHeader))
        containerRequestContext.getHeaders() >> map
        containerRequestContext.getSecurityContext() >> sc

        when:
        raf.filter(containerRequestContext)

        then:
        WebApplicationException wae = thrown(WebApplicationException)
        Response response = wae.getResponse()
        response.getStatus() == 403
        ((String) response.getEntity()).contains("without proper authorization")
        1 * isAuthorized.test(sc) >> false
    }

    def "Malformed from throws exception"() {
        setup:
        String fromHeader = "user1!here.there"
        MultivaluedMap map = new MultivaluedHashMap()
        map.put("From", Collections.singletonList(fromHeader))
        containerRequestContext.getHeaders() >> map
        containerRequestContext.getSecurityContext() >> sc

        when:
        raf.filter(containerRequestContext)

        then:
        WebApplicationException wae = thrown(WebApplicationException)
        Response response = wae.getResponse()
        response.getStatus() == 401
        ((String) response.getEntity()).contains("Expected USERID@DOMAIN")
        1 * isAuthorized.test(sc) >> true

        where:
        header | foo
        "user!here.there" | _
        "@here.there" | _

    }

    def "Fetch user roles error is propagated"() {
        setup:
        RoleImpersonationFilter spyRaf = Spy(raf)
        spyRaf.getRolesForId(_) >> {throw new IOException("Something went wrong")}
        String fromHeader = "user1@here.there"
        MultivaluedMap map = new MultivaluedHashMap()
        map.put("From", Collections.singletonList(fromHeader))
        containerRequestContext.getHeaders() >> map
        containerRequestContext.getSecurityContext() >> sc

        when:
        spyRaf.filter(containerRequestContext)

        then:
        WebApplicationException wae = thrown(WebApplicationException)
        Response response = wae.getResponse()
        response.getStatus() == 403
        ((String) response.getEntity()).contains("Expected USERID@DOMAIN")
        1 * isAuthorized.test(sc) >> true
    }
}
