package org.apache.lucene.search;

import org.apache.lucene.index.Term;

public class PrefixLengthQuery extends MultiTermQueryTermEnumLengthFilter {

    private final int minLength;

    private final int maxLength;

    public PrefixLengthQuery(Term prefix, int minLength, int maxLength) {
        super(new PrefixQuery(prefix), minLength, maxLength);
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public int getMinLength() {
        return minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getFilteredQuery().toString(field));
        buffer.append("{length:");
        buffer.append(minLength);
        buffer.append(" TO ");
        buffer.append(maxLength);
        buffer.append('}');
        return buffer.toString();
    }

}
