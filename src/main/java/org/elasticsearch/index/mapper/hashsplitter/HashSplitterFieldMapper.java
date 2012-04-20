/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.hashsplitter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchNoDocsFilter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.PrefixLengthFilter;
import org.apache.lucene.search.PrefixLengthQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeLengthFilter;
import org.apache.lucene.search.WildcardFilter;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.HashSplitterAnalyzer;
import org.elasticsearch.index.analysis.HashSplitterSearchAnalyzer;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

public class HashSplitterFieldMapper extends StringFieldMapper implements CustomWildcardSearchFieldMapper {

    public static final String CONTENT_TYPE = "hashsplitter";

    public static HashSplitterFieldMapper.Builder hashSplitterField(String name) {
        return new HashSplitterFieldMapper.Builder(name);
    }

    public static class Defaults {
        public static final Boolean INCLUDE_IN_ALL = null;
        public static final Field.Index INDEX = Field.Index.ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final int CHUNK_LENGTH = 1;
        public static final String PREFIXES = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789,.";
        public static final Integer SIZE = null;
        public static final char WILDCARD_ONE = WildcardQuery.DEFAULT_WILDCARD_ONE;
        public static final char WILDCARD_ANY = WildcardQuery.DEFAULT_WILDCARD_ANY;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, HashSplitterFieldMapper> {

        protected String nullValue = StringFieldMapper.Defaults.NULL_VALUE;

        private int chunkLength = Defaults.CHUNK_LENGTH;

        private String prefixes = Defaults.PREFIXES;

        private Integer size = Defaults.SIZE;

        private char wildcardOne = Defaults.WILDCARD_ONE;

        private char wildcardAny = Defaults.WILDCARD_ANY;

        public Builder(String name) {
            super(name);
            super.includeInAll(Defaults.INCLUDE_IN_ALL);
            super.index(Defaults.INDEX);
            super.store(Defaults.STORE);
            builder = this;
        }

        @Override
        public Builder index(Field.Index index) {
            return super.index(index);
        }

        @Override
        public Builder store(Field.Store store) {
            return super.store(store);
        }

        @Override
        public Builder boost(float boost) {
            return super.boost(boost);
        }

        @Override
        public Builder indexName(String indexName) {
            return super.indexName(indexName);
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public Builder includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return this;
        }

        public Builder chunkLength(int chunkLen) {
            this.chunkLength = chunkLen;
            return this;
        }

        public Builder prefixes(String prefixes) {
            this.prefixes = prefixes;
            return this;
        }

        public Builder size(Integer size) {
            this.size = size;
            return this;
        }

        public Builder wildcardOne(char wildcardOne) {
            this.wildcardOne = wildcardOne;
            return this;
        }

        public Builder wildcardAny(char wildcardAny) {
            this.wildcardAny = wildcardAny;
            return this;
        }

        @Override
        public HashSplitterFieldMapper build(BuilderContext context) {
            HashSplitterFieldMapper fieldMapper = new HashSplitterFieldMapper(buildNames(context),
                    index, store, boost, nullValue,
                    chunkLength, prefixes, size == null, size == null ? -1 : size.intValue(),
                    wildcardOne, wildcardAny);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    /**
     * <pre>
     *  field1 : { type : "hashsplitter" }
     * </pre>
     * Or:
     * <pre>
     *  field1 : {
     *      type : "hashsplitter",
     *      settings: {
     *          chunk_length : 1,
     *          prefixes : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789,."
     *          size : "variable" | 32,
     *          wildcard_one : "?",
     *          wildcard_any : "*"
     *      }
     * }
     * </pre>
     */
    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            HashSplitterFieldMapper.Builder builder = hashSplitterField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("null_value")) {
                    builder.nullValue(fieldNode.toString());
                } else if (fieldName.equals("settings")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Map.Entry<String, Object> entry1 : fieldsNode.entrySet()) {
                        String propName = entry1.getKey();
                        Object propNode = entry1.getValue();

                        if ("chunk_length".equals(propName)) {
                            builder.chunkLength(nodeIntegerValue(propNode));
                        } else if ("prefixes".equals(propName)) {
                            builder.prefixes(propNode.toString());
                        } else if ("size".equals(propName)) {
                            builder.size(nodeSizeValue(propNode));
                        } else if ("wildcard_one".equals(propName)) {
                            String value = propNode.toString();
                            if (value.length() != 1)
                                throw new MapperParsingException("["+HashSplitterFieldMapper.CONTENT_TYPE+"] Field "+name+" only supports 1-character long wildcard_one");
                            builder.wildcardOne(value.charAt(0));
                        } else if ("wildcard_any".equals(propName)) {
                            String value = propNode.toString();
                            if (value.length() != 1)
                                throw new MapperParsingException("["+HashSplitterFieldMapper.CONTENT_TYPE+"] Field "+name+" only supports 1-character long wildcard_any");
                            builder.wildcardAny(value.charAt(0));
                        }
                    }
                }
            }
            return builder;
        }

        public static Integer nodeSizeValue(Object node) {
            if (node instanceof Number) {
                if (node instanceof Integer)
                    return (Integer) node;
                return ((Number) node).intValue();
            }
            try {
                return Integer.valueOf(node.toString());
            } catch (NumberFormatException ex) {
                return null;
            }
        }

    }

    protected String nullValue;

    protected Boolean includeInAll;

    protected int chunkLength;

    protected String prefixes;

    protected boolean sizeIsVariable;

    protected int sizeValue;

    protected char wildcardOne;

    protected char wildcardAny;

    protected HashSplitterAnalyzer indexAnalyzer;

    protected HashSplitterSearchAnalyzer searchAnalyzer;

    public HashSplitterFieldMapper(Names names, Field.Index index, Field.Store store, float boost, String nullValue,
                                   int chunkLength, String prefixes, boolean sizeIsVariable, int sizeValue,
                                   char wildcardOne, char wildcardAny) {
        super(names, index, store, Field.TermVector.NO, boost, true, true, nullValue, null, null);
        this.nullValue = nullValue;
        this.includeInAll = null;
        this.chunkLength = chunkLength;
        this.prefixes = prefixes;
        this.sizeIsVariable = sizeIsVariable;
        this.sizeValue = sizeValue;
        this.wildcardOne = wildcardOne;
        this.wildcardAny = wildcardAny;
        this.indexAnalyzer = new HashSplitterAnalyzer(this.chunkLength, this.prefixes);
        this.searchAnalyzer = new HashSplitterSearchAnalyzer(this.chunkLength, this.prefixes, this.wildcardOne, this.wildcardAny, this.sizeIsVariable, this.sizeValue);
    }

    @Override
    public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
        // Keep underlying, private includeInAll field in sync
        super.includeInAll(includeInAll);
    }

    @Override
    public void includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
        // Keep underlying, private includeInAll field in sync
        super.includeInAllIfNotSet(includeInAll);
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            HashSplitterFieldMapper casted = (HashSplitterFieldMapper) mergeWith;
            this.includeInAll = casted.includeInAll;
            this.nullValue = casted.nullValue;
            this.chunkLength = casted.chunkLength;
            this.prefixes = casted.prefixes;
            this.sizeIsVariable = casted.sizeIsVariable;
            this.sizeValue = casted.sizeValue;
            this.wildcardOne = casted.wildcardOne;
            this.wildcardAny = casted.wildcardAny;
            this.indexAnalyzer = new HashSplitterAnalyzer(this.chunkLength, this.prefixes);
            this.searchAnalyzer = new HashSplitterSearchAnalyzer(this.chunkLength, this.prefixes, this.wildcardOne, this.wildcardAny, this.sizeIsVariable, this.sizeValue);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder) throws IOException {
        // As we change the default values for many fields,
        // it may be more interesting not to inherit StringFieldMapper but AbstractFieldMapper directly,
        //super.doXContentBody(builder);

        // From AbstractFieldMapper
        builder.field("type", contentType());
        if (!names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        // From StringFieldMapper
        if (index != Defaults.INDEX) {
            builder.field("index", index.name().toLowerCase());
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }

        // Additions
        if (chunkLength != Defaults.CHUNK_LENGTH || sizeIsVariable != (Defaults.SIZE == null)
                || Defaults.SIZE != null && sizeValue != Defaults.SIZE.intValue()
                || wildcardOne != Defaults.WILDCARD_ONE || wildcardAny != Defaults.WILDCARD_ANY) {
            builder.startObject("settings");
            if (chunkLength != Defaults.CHUNK_LENGTH) {
                builder.field("chunk_length", chunkLength);
            }
            if (prefixes != Defaults.PREFIXES) {
                builder.field("prefixes", prefixes);
            }
            if (sizeIsVariable != (Defaults.SIZE == null)
                    || Defaults.SIZE != null && sizeValue != Defaults.SIZE.intValue()) {
                builder.field("size");
                if (sizeIsVariable)
                    builder.value("variable");
                else
                    builder.value(sizeValue);
            }
            if (wildcardOne != Defaults.WILDCARD_ONE) {
                builder.field("wildcard_one", Character.toString(wildcardOne));
            }
            if (wildcardAny != Defaults.WILDCARD_ANY) {
                builder.field("wildcard_any", Character.toString(wildcardAny));
            }
            builder.endObject();
        }
    }

    @Override
    public Analyzer indexAnalyzer() {
        return indexAnalyzer;
    }

    @Override
    public Analyzer searchAnalyzer() {
        return searchAnalyzer;
    }

    @Override
    public boolean useFieldQueryWithQueryString() {
        // Don't use our search analyzer with all query strings, prefer our overloaded fieldQuery()
        // (See elasticsearch org.apache.lucene.queryParser.MapperQueryParser.getFieldQuery()'s call to this function.)
        return true;
    }

    @Override
    public Query fieldQuery(String value, @Nullable QueryParseContext context) {
        // Use HashSplitterSearch* analysis and post-process it to create the real query
        TokenStream tok = null;
        try {
            tok = indexAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanQuery q = new BooleanQuery();
        try {
            while (tok.incrementToken()) {
                Term term = names().createIndexNameTerm(termAtt.toString());
                q.add(new TermQuery(term), BooleanClause.Occur.MUST);
            }
            tok.end();
            tok.close();
        } catch (IOException e) {
            e.printStackTrace();
            q = null;
        }
        return q;
    }

    @Override
    public Filter fieldFilter(String value, @Nullable QueryParseContext context) {
        // Use HashSplitterSearch* analysis and post-process it to create the real query
        TokenStream tok = null;
        try {
            tok = indexAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanFilter f = new BooleanFilter();
        try {
            while (tok.incrementToken()) {
                Term term = names().createIndexNameTerm(termAtt.toString());
                f.add(new TermFilter(term), BooleanClause.Occur.MUST);
            }
            tok.end();
            tok.close();
        } catch (IOException e) {
            e.printStackTrace();
            f = null;
        }
        return f;
    }

    @Override
    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        // Use HashSplitterSearch* analysis and post-process it to create the real query
        TokenStream tok = null;
        try {
            tok = indexAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanQuery q = new BooleanQuery();
        try {
            int remainingSize = sizeIsVariable ? 0 : sizeValue; // note: prefixes are not included
            while (tok.incrementToken()) {
                Term term = names().createIndexNameTerm(termAtt.toString());
                if (termAtt.length() < 1 + chunkLength) {
                    if (remainingSize > 0) { // implies size is fixed
                        if (remainingSize < chunkLength)
                            q.add(new PrefixLengthQuery(term, 1 + remainingSize, 1 + remainingSize), BooleanClause.Occur.MUST);
                        else
                            q.add(new PrefixLengthQuery(term, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.MUST);
                    } else { // varying size: only limit to the chunkLength
                        q.add(new PrefixLengthQuery(term, 0, 1 + chunkLength), BooleanClause.Occur.MUST);
                    }
                } else {
                    q.add(new TermQuery(term), BooleanClause.Occur.MUST);
                }
                remainingSize -= termAtt.length() - 1; // termAtt contains the prefix, remainingSize doesn't take it into account
            }
            tok.end();
            tok.close();
        } catch (IOException e) {
            e.printStackTrace();
            q = null;
        }
        return q;
    }

    @Override
    public Filter prefixFilter(String value, @Nullable QueryParseContext context) {
        // Use HashSplitterSearch* analysis and post-process it to create the real filter
        TokenStream tok = null;
        try {
            tok = indexAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanFilter f = new BooleanFilter();
        try {
            int remainingSize = sizeIsVariable ? 0 : sizeValue; // note: prefixes are not included
            while (tok.incrementToken()) {
                Term term = names().createIndexNameTerm(termAtt.toString());
                if (termAtt.length() < 1 + chunkLength) {
                    if (remainingSize > 0) { // implies size is fixed
                        if (remainingSize < chunkLength)
                            f.add(new PrefixLengthFilter(term, 1 + remainingSize, 1 + remainingSize), BooleanClause.Occur.MUST);
                        else
                            f.add(new PrefixLengthFilter(term, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.MUST);
                    } else { // varying size: only limit to the chunkLength
                        f.add(new PrefixLengthFilter(term, 0, 1 + chunkLength), BooleanClause.Occur.MUST);
                    }
                } else {
                    f.add(new TermFilter(term), BooleanClause.Occur.MUST);
                }
                remainingSize -= termAtt.length() - 1; // termAtt contains the prefix, remainingSize doesn't take it into account
            }
            tok.end();
            tok.close();
        } catch (IOException e) {
            e.printStackTrace();
            f = null;
        }
        return f;
    }

    @Override
    public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        // Delegate to rangeFilter(), and wrap it into a query
        Filter filter = rangeFilter(lowerTerm, upperTerm, includeLower, includeUpper, context);
        if (filter == null)
            return null;
        return new ConstantScoreQuery(filter);
    }

    @Override
    public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        // Special case: -infinity to +infinity
        if (lowerTerm == null && upperTerm == null) {
            if (sizeIsVariable)
                return null;
            StringBuilder sbWildcardPart = new StringBuilder();
            for (int i = 0 ; i < chunkLength ; i++)
                sbWildcardPart.append(wildcardOne);
            String wildcardPart = sbWildcardPart.toString();
            BooleanFilter filter = new BooleanFilter();
            for (int i = sizeValue / chunkLength - 1 ; i >= 0 ; i--) {
                filter.add(new WildcardFilter(names().createIndexNameTerm(prefixes.charAt(i) + wildcardPart)), BooleanClause.Occur.MUST);
            }
            if (sizeValue % chunkLength != 0) {
                // If the size is not dividible by chunkLength,
                // we still have a last chunk, but that has a shorter length
                filter.add(new WildcardFilter(names().createIndexNameTerm(prefixes.charAt(sizeValue/chunkLength+1) + wildcardPart.substring(0, sizeValue % chunkLength))), BooleanClause.Occur.MUST);
            }
            return filter;
        }
        // Check for emptyness
        if (lowerTerm != null && upperTerm != null) {
            int cmp = lowerTerm.compareTo(upperTerm);
            // Bound invertion
            if (cmp > 0)
                return MatchNoDocsFilter.INSTANCE;
            // Equal bounds
            if (cmp == 0) {
                // and both inclusive bounds: singleton
                if (includeLower && includeUpper) {
                    // Special case: equal terms
                    return fieldFilter(lowerTerm, context);
                }
                // otherwise, empty range
                return MatchNoDocsFilter.INSTANCE;
            }
        }
        // Analyze lower and upper terms
        List<String> lowerTerms = new LinkedList<String>();
        List<String> upperTerms = new LinkedList<String>();
        if (lowerTerm != null) {
            TokenStream tok = null;
            try {
                tok = indexAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(lowerTerm));
                tok.reset();
            } catch (IOException e) {
                return null;
            }
            CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
            try {
                while (tok.incrementToken())
                    lowerTerms.add(termAtt.toString());
                tok.end();
                tok.close();
            } catch (IOException e) {
                return null;
            }
        }
        if (upperTerm != null) {
            TokenStream tok = null;
            try {
                tok = indexAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(upperTerm));
                tok.reset();
            } catch (IOException e) {
                return null;
            }
            CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
            try {
                while (tok.incrementToken())
                    upperTerms.add(termAtt.toString());
                tok.end();
                tok.close();
            } catch (IOException e) {
                return null;
            }
        }
        // Generate the filter
        BooleanFilter topLevelAndFilter = new BooleanFilter();
        Iterator<String> lowers = lowerTerms.iterator();
        Iterator<String> uppers = upperTerms.iterator();
        String currLower = null;
        String currUpper = null;
        int remainingLowerSize = sizeIsVariable ? 0 : sizeValue;
        int remainingUpperSize = sizeIsVariable ? 0 : sizeValue;

        // First, the common prefix
        while (lowers.hasNext() && uppers.hasNext()) {
            currLower = lowers.next();
            currUpper = uppers.next();
            // The last part cannot be part of the prefix
            // because that special case has already been handled
            if (!lowers.hasNext() || !uppers.hasNext())
                break;
            if (!currLower.equals(currUpper))
                break;
            topLevelAndFilter.add(new TermFilter(names().createIndexNameTerm(currLower)), BooleanClause.Occur.MUST);
            remainingLowerSize -= currLower.length() - 1;
            remainingUpperSize -= currUpper.length() - 1;
        }

        String subPrefixLower = currLower;
        BooleanFilter secondLevelOrFilter = new BooleanFilter();
        BooleanFilter lastFilter;
        // Add the range part of the query (secondLevelOrFilter) to the prefix part is already in topLevelAndFilter
        topLevelAndFilter.add(secondLevelOrFilter, BooleanClause.Occur.MUST);
        // We still have secondLevelOrFilter to populate

        lastFilter = new BooleanFilter();
        // Handle the first diverging token of the lowerTerm (if it's not also the last available!)
        if (lowers.hasNext()) {
            lastFilter.add(new TermFilter(names().createIndexNameTerm(currLower)), BooleanClause.Occur.MUST);
            remainingLowerSize -= currLower.length() - 1;
            currLower = lowers.next();
        }
        secondLevelOrFilter.add(lastFilter, BooleanClause.Occur.SHOULD);
        // Then get to the last token of the lowerTerm
        while (lowers.hasNext()) {
            BooleanFilter orFilter = new BooleanFilter();
            lastFilter.add(orFilter, BooleanClause.Occur.MUST);
            orFilter.add(new TermRangeLengthFilter(names().indexName(), currLower, luceneTermUpperBound(currLower), false, false, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.SHOULD);
            BooleanFilter nextFilter = new BooleanFilter();
            nextFilter.add(new TermFilter(names().createIndexNameTerm(currLower)), BooleanClause.Occur.MUST);
            orFilter.add(nextFilter, BooleanClause.Occur.SHOULD);
            lastFilter = nextFilter;
            remainingLowerSize -= currLower.length() - 1;
            currLower = lowers.next();
        }
        // Handle the last token of the lowerTerm
        if (remainingLowerSize < 0)
            lastFilter.add(new TermRangeLengthFilter(names().indexName(), currLower, luceneTermUpperBound(currLower), includeLower, false, 0, 1 + chunkLength), BooleanClause.Occur.MUST);
        else if (remainingLowerSize < chunkLength)
            lastFilter.add(new TermRangeLengthFilter(names().indexName(), currLower, luceneTermUpperBound(currLower), includeLower, false, 1 + remainingLowerSize, 1 + remainingLowerSize), BooleanClause.Occur.MUST);
        else
            lastFilter.add(new TermRangeLengthFilter(names().indexName(), currLower, luceneTermUpperBound(currLower), includeLower, false, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.MUST);

        // Range from the non prefix part of the lowerTerm to the non prefix part of the upperTerm
        if (remainingUpperSize < 0)
            secondLevelOrFilter.add(new TermRangeLengthFilter(names().indexName(), subPrefixLower, currUpper, false, false, 0, 1 + chunkLength), BooleanClause.Occur.SHOULD);
        else if (remainingUpperSize < chunkLength)
            secondLevelOrFilter.add(new TermRangeLengthFilter(names().indexName(), subPrefixLower, currUpper, false, false, 1 + remainingUpperSize, 1 + remainingUpperSize), BooleanClause.Occur.SHOULD);
        else
            secondLevelOrFilter.add(new TermRangeLengthFilter(names().indexName(), subPrefixLower, currUpper, false, false, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.SHOULD);

        lastFilter = new BooleanFilter();
        // Handle the first diverging token of the upperTerm (if it's not also the last available!)
        if (uppers.hasNext()) {
            lastFilter.add(new TermFilter(names().createIndexNameTerm(currUpper)), BooleanClause.Occur.MUST);
            remainingUpperSize -= currUpper.length() - 1;
            currUpper = uppers.next();
        }
        secondLevelOrFilter.add(lastFilter, BooleanClause.Occur.SHOULD);
        // Then get to the last token of the upperTerm
        while (uppers.hasNext()) {
            BooleanFilter orFilter = new BooleanFilter();
            lastFilter.add(orFilter, BooleanClause.Occur.MUST);
            orFilter.add(new TermRangeLengthFilter(names().indexName(), luceneTermLowerBound(currUpper), currUpper, false, false, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.SHOULD);
            BooleanFilter nextFilter = new BooleanFilter();
            nextFilter.add(new TermFilter(names().createIndexNameTerm(currUpper)), BooleanClause.Occur.MUST);
            orFilter.add(nextFilter, BooleanClause.Occur.SHOULD);
            lastFilter = nextFilter;
            remainingUpperSize -= currUpper.length() - 1;
            currUpper = uppers.next();
        }
        // Handle the last token of the upperTerm
        if (remainingUpperSize < 0)
            lastFilter.add(new TermRangeLengthFilter(names().indexName(), luceneTermLowerBound(currUpper), currUpper, false, includeUpper, 0, 1 + chunkLength), BooleanClause.Occur.MUST);
        else if (remainingUpperSize < chunkLength)
            lastFilter.add(new TermRangeLengthFilter(names().indexName(), luceneTermLowerBound(currUpper), currUpper, false, includeUpper, 1 + remainingUpperSize, 1 + remainingUpperSize), BooleanClause.Occur.MUST);
        else
            lastFilter.add(new TermRangeLengthFilter(names().indexName(), luceneTermLowerBound(currUpper), currUpper, false, includeUpper, 1 + chunkLength, 1 + chunkLength), BooleanClause.Occur.MUST);

        return topLevelAndFilter;
    }
    private String luceneTermUpperBound(String term) {
        // Lucene terms are ordered lexicographically (by UTF-16 character code)
        // so we should not use:
        //   return Character.toString(prefixes.charAt((prefixes.indexOf(term.charAt(0))+1)%prefixes.length()));
        // that will get the next *prefix* (the prefix for the *lower level*)
        // but merely the next *char*!
        return Character.toString((char) (term.charAt(0) + 1));
    }
    private String luceneTermLowerBound(String term) {
        // Simply the prefix alone
        return term.substring(0, 1);
    }

    @Override
    public Query queryStringTermQuery(Term term) {
        // Direct use of term queries against such a field implies you know what you are doing
        // (ie. the use of prefixes and chunks)
        // We could also decide to call over to fieldquery().
        return null;
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions) {
        // Not supported for now
        return null; // will fallback to an unusable default query
    }

    @Override
    public Query fuzzyQuery(String value, double minSim, int prefixLength, int maxExpansions) {
        // Not supported for now
        return null; // will fallback to an unusable default query
    }

    @Override
    public Query wildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        // Use HashSplitterSearch* analysis and post-process it to create the real query
        TokenStream tok = null;
        try {
            tok = searchAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanQuery q = new BooleanQuery();
        try {
            while (tok.incrementToken()) {
                q.add(new WildcardQuery(names().createIndexNameTerm(termAtt.toString()), wildcardOne, wildcardAny), BooleanClause.Occur.MUST);
            }
            tok.end();
            tok.close();
        } catch (IOException e) {
            e.printStackTrace();
            q = null;
        }
        return q;
    }

    @Override
    public Filter wildcardFilter(String value, @Nullable QueryParseContext context) {
        // Use HashSplitterSearch* analysis and post-process it to create the real query
        TokenStream tok = null;
        try {
            tok = searchAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanFilter f = new BooleanFilter();
        try {
            while (tok.incrementToken()) {
                f.add(new WildcardFilter(names().createIndexNameTerm(termAtt.toString()), wildcardOne, wildcardAny), BooleanClause.Occur.MUST);
            }
            tok.end();
            tok.close();
        } catch (IOException e) {
            e.printStackTrace();
            f = null;
        }
        return f;
    }

}
