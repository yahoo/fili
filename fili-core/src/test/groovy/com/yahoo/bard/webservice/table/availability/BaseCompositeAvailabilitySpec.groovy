package com.yahoo.bard.webservice.table.availability

import spock.lang.Specification

import java.util.stream.Stream

class BaseCompositeAvailabilitySpec extends Specification {

    /**
     * Simple class extending BaseCompositeAvailability to allow for testing of its methods
     */
    class SimpleCompositeAvailability extends BaseCompositeAvailability {

        /**
         * Constructor.
         *
         * @param availabilityStream A potentially ordered stream of availabilities which supply this composite view
         */
        protected SimpleCompositeAvailability(Stream<Availability> availabilityStream) {
            super(availabilityStream)
        }
    }

    def setup() {

    }
}
