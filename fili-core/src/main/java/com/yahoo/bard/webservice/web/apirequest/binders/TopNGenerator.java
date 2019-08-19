package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOP_N_UNSORTED;

import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import java.util.LinkedHashSet;

public interface TopNGenerator {
    int generateTopN(String topN);

    void validateTopN(String topNRequest,  int topN, LinkedHashSet<OrderByColumn> sorts);

    TopNGenerator DEFAULT_TOP_N_GENERATOR = new TopNGenerator() {
        @Override
        public int generateTopN(final String topN) {
            try {
                return topN == null ? 0 : Integer.parseInt(topN);
            } catch (NumberFormatException nfe) {
                throw new BadApiRequestException(INTEGER_INVALID.logFormat(topN, "topN"), nfe);
            }
        }

        @Override
        public void validateTopN(String topNRequest, int topN, LinkedHashSet<OrderByColumn> sorts) {
            // This is the validation part for topN that is inlined here because currently it is very brief.
            if (topN < 0) {
                throw new BadApiRequestException(INTEGER_INVALID.logFormat(topNRequest, "topN"));
            } else if (topN > 0 && sorts.isEmpty()) {
                throw new BadApiRequestException(TOP_N_UNSORTED.format(topNRequest));
            }
        }
    };
}
