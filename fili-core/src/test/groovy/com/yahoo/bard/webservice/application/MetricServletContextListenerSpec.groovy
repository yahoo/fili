// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import com.codahale.metrics.servlet.InstrumentedFilter

import spock.lang.Specification

import javax.servlet.ServletContext
import javax.servlet.ServletContextEvent

class MetricServletContextListenerSpec extends Specification {

    def "Initialize Filter"() {
        when:
        ServletContextEvent event = Mock(ServletContextEvent)
        ServletContext sc = Mock(ServletContext)
        MetricServletContextListener cl = new MetricServletContextListener()
        event.getServletContext() >> sc
        1 * sc.setAttribute(InstrumentedFilter.REGISTRY_ATTRIBUTE, _ as Object)

        then:
        cl.contextInitialized(event)
    }
}
