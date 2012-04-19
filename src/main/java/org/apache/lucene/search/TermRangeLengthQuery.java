package org.apache.lucene.search;

public class TermRangeLengthQuery extends MultiTermQueryTermEnumLengthFilter {

    public TermRangeLengthQuery(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, int minLength, int maxLength) {
        super(new TermRangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper), minLength, maxLength);
    }

}
