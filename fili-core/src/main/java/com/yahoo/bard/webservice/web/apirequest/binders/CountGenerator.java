package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID;

import com.yahoo.bard.webservice.web.BadApiRequestException;

public interface CountGenerator {
    int generateCount(String count);

    void validateCount(String countRequest, int count);

    CountGenerator DEFAULT_COUNT_GENERATOR = new CountGenerator() {
        @Override
        public int generateCount(String count) {
            try {
                return count == null ? 0 : Integer.parseInt(count);
            } catch (NumberFormatException nfe) {
                throw new BadApiRequestException(INTEGER_INVALID.logFormat(count, "count"), nfe);
            }
        }

        @Override
        public void validateCount(final String countRequest, final int count) {
            // This is the validation part for count that is inlined here because currently it is very brief.
            if (count < 0) {
                throw new BadApiRequestException(INTEGER_INVALID.logFormat(countRequest, "count"));
            }
        }
    };
}
