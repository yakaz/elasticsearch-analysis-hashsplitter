package org.apache.lucene.search;

public class TermRangeLengthFilter extends MultiTermQueryWrapperFilter<MultiTermQueryTermEnumLengthFilter> {

    public TermRangeLengthFilter(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, int minLength, int maxLength) {
        super(new MultiTermQueryTermEnumLengthFilter(new TermRangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper), minLength, maxLength));
    }

}
