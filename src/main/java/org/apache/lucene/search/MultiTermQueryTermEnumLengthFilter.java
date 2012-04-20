package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;

/**
 * A length filter on the terms enumerated by the filtered MultiTermQuery.
 */
public class MultiTermQueryTermEnumLengthFilter extends MultiTermQuery {

    private final MultiTermQuery filteredQuery;
    private final int minLength;
    private final int maxLength;

    public MultiTermQueryTermEnumLengthFilter(MultiTermQuery filteredQuery, int minLength, int maxLength) {
        this.filteredQuery = filteredQuery;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public MultiTermQueryWrapperFilter<MultiTermQueryTermEnumLengthFilter> wrapAsFilter() {
        return new MultiTermQueryWrapperFilter<MultiTermQueryTermEnumLengthFilter>(this);
    }

    public MultiTermQuery getFilteredQuery() {
        return filteredQuery;
    }

    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
        return new LengthFilteredTermEnum(filteredQuery.getEnum(reader), minLength, maxLength);
    }

    @Override
    public String toString(String field) {
        return "MultiTermQueryTermEnumLengthFilter(" + filteredQuery.toString(field) + ", length:[" + minLength + " TO " + maxLength + "])";
    }

    public static class LengthFilteredTermEnum extends FilteredTermEnum {

        private final FilteredTermEnum filteredEnum;
        private final int minLength;
        private final int maxLength;

        public LengthFilteredTermEnum(FilteredTermEnum filteredEnum, int minLength, int maxLength) {
            this.filteredEnum = filteredEnum;
            this.minLength = minLength;
            this.maxLength = maxLength;

            // Check the first term is acceptable, or seek to the next acceptable one
            if (filteredEnum.term() != null) {
                int len = filteredEnum.term().text().length();
                if (len < minLength || len > maxLength) {
                    try {
                        next();
                    } catch (IOException e) {
                    }
                }
            }
        }

        @Override
        protected boolean termCompare(Term term) {
            return filteredEnum.termCompare(term);
        }

        @Override
        public float difference() {
            return filteredEnum.difference();
        }

        @Override
        protected boolean endEnum() {
            return filteredEnum.endEnum();
        }

        @Override
        public boolean next() throws IOException {
            int len;
            do {
                if (!filteredEnum.next())
                    return false;
                len = filteredEnum.term().text().length();
            } while (len < minLength || len > maxLength);
            return true;
        }

        @Override
        protected void setEnum(TermEnum actualEnum) throws IOException {
            filteredEnum.setEnum(actualEnum);
        }

        @Override
        public int docFreq() {
            return filteredEnum.docFreq();
        }

        @Override
        public Term term() {
            return filteredEnum.term();
        }

        @Override
        public void close() throws IOException {
            filteredEnum.close();
        }
    }

}
