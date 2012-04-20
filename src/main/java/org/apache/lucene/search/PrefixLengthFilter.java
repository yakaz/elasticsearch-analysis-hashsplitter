package org.apache.lucene.search;

import org.apache.lucene.index.Term;

public class PrefixLengthFilter extends MultiTermQueryWrapperFilter<MultiTermQueryTermEnumLengthFilter> {

    private final int minLength;

    private final int maxLength;

    public PrefixLengthFilter(Term prefix, int minLength, int maxLength) {
        super(new MultiTermQueryTermEnumLengthFilter(new PrefixQuery(prefix), minLength, maxLength));
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
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(query.getFilteredQuery().toString());
        buffer.append("{length:");
        buffer.append(minLength);
        buffer.append(" TO ");
        buffer.append(maxLength);
        buffer.append('}');
        return buffer.toString();
    }

}
