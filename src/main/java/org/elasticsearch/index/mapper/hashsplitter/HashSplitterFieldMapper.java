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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

public class HashSplitterFieldMapper extends StringFieldMapper {

    public static final String CONTENT_TYPE = "hashsplitter";

    public static HashSplitterFieldMapper.Builder hashSplitterField(String name) {
        return new HashSplitterFieldMapper.Builder(name);
    }

    public static class Defaults {
        public static final Boolean INCLUDE_IN_ALL = null;
        public static final Field.Index INDEX = Field.Index.ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final int CHUNK_LENGTH = 1;
        public static final Integer SIZE = null;
        public static final char WILDCARD_ONE = '?';
        public static final char WILDCARD_ANY = '*';
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, HashSplitterFieldMapper> {

        protected String nullValue = StringFieldMapper.Defaults.NULL_VALUE;

        private int chunkLength = Defaults.CHUNK_LENGTH;

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

        public Builder size(Integer size) {
            this.size = size;
            return this;
        }

        public Builder wildcardOne(String wildcardOne) {
            if (wildcardOne != null && wildcardOne.length() == 1)
                this.wildcardOne = wildcardOne.charAt(0);
            return this;
        }

        public Builder wildcardAny(String wildcardAny) {
            if (wildcardAny != null && wildcardAny.length() == 1)
                this.wildcardAny = wildcardAny.charAt(0);
            return this;
        }

        @Override
        public HashSplitterFieldMapper build(BuilderContext context) {
            HashSplitterFieldMapper fieldMapper = new HashSplitterFieldMapper(buildNames(context),
                    index, store, boost, nullValue,
                    chunkLength, size == null, size == null ? -1 : size.intValue(),
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
     *          chunk_length : 2,
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
                        } else if ("size".equals(propName)) {
                            builder.size(nodeSizeValue(propNode));
                        } else if ("wildcard_one".equals(propName)) {
                            builder.wildcardOne(propNode.toString());
                        } else if ("wildcard_any".equals(propName)) {
                            builder.wildcardAny(propNode.toString());
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
            if ("variable".equals(node.toString()))
                return null;
            return Integer.getInteger(node.toString());
        }

    }

    protected String nullValue;

    protected Boolean includeInAll;

    protected int chunkLength;
    
    protected boolean sizeIsVariable;

    protected int sizeValue;

    protected char wildcardOne;

    protected char wildcardAny;
    
    protected HashSplitterAnalyzer indexAnalyzer;

    protected HashSplitterSearchAnalyzer searchAnalyzer;

    public HashSplitterFieldMapper(Names names, Field.Index index, Field.Store store, float boost, String nullValue,
                                   int chunkLength, boolean sizeIsVariable, int sizeValue,
                                   char wildcardOne, char wildcardAny) {
        super(names, index, store, Field.TermVector.NO, boost, true, true, nullValue, null, null);
        this.nullValue = nullValue;
        this.includeInAll = null;
        this.chunkLength = chunkLength;
        this.sizeIsVariable = sizeIsVariable;
        this.sizeValue = sizeValue;
        this.wildcardOne = wildcardOne;
        this.wildcardAny = wildcardAny;
        this.indexAnalyzer = new HashSplitterAnalyzer(this.chunkLength);
        this.searchAnalyzer = new HashSplitterSearchAnalyzer(this.chunkLength, HashSplitterSearchAnalyzer.DEFAULT_PREFIXES, this.wildcardOne, this.wildcardAny, this.sizeIsVariable, this.sizeValue);
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
            this.sizeIsVariable = casted.sizeIsVariable;
            this.sizeValue = casted.sizeValue;
            this.wildcardOne = casted.wildcardOne;
            this.wildcardAny = casted.wildcardAny;
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
            if (sizeIsVariable != (Defaults.SIZE == null)
                    || Defaults.SIZE != null && sizeValue != Defaults.SIZE.intValue()) {
                builder.field("size");
                if (sizeIsVariable)
                    builder.value("variable");
                else
                    builder.value(sizeValue);
            }
            if (wildcardOne != Defaults.WILDCARD_ONE) {
                builder.field("wildcard_one", wildcardOne);
            }
            if (wildcardAny != Defaults.WILDCARD_ANY) {
                builder.field("wildcard_any", wildcardAny);
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
    public Query fieldQuery(String value, @Nullable QueryParseContext context) {
        // TODO Expand "*" and use special HashSplitterSearch* analysis and post-process it to create real queries
        return super.fieldQuery(value, context);
    }

    @Override
    public Filter fieldFilter(String value, @Nullable QueryParseContext context) {
        // TODO Expand "*" and use special HashSplitterSearch* analysis and post-process it to create real queries
        return super.fieldFilter(value, context);
    }

    @Override
    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
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
                Term term = names().createIndexNameTerm(termAtt.toString());
                if (termAtt.length() < 1 + chunkLength) {
                    q.add(new PrefixQuery(term), BooleanClause.Occur.MUST);
                } else {
                    q.add(new TermQuery(term), BooleanClause.Occur.MUST);
                }
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
            tok = searchAnalyzer.reusableTokenStream(names().indexNameClean(), new FastStringReader(value));
            tok.reset();
        } catch (IOException e) {
            return null;
        }
        CharTermAttribute termAtt = tok.getAttribute(CharTermAttribute.class);
        BooleanFilter q = new BooleanFilter();
        try {
            while (tok.incrementToken()) {
                Term term = names().createIndexNameTerm(termAtt.toString());
                if (termAtt.length() < 1 + chunkLength) {
                    q.add(new PrefixFilter(term), BooleanClause.Occur.MUST);
                } else {
                    q.add(new TermFilter(term), BooleanClause.Occur.MUST);
                }
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
    public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        // TODO Remove final "*" and use special HashSplitterSearch* analysis and post-process it to create real queries
        // TODO get inspiration from NumericRangeQuery
        return super.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, context);
    }

    @Override
    public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        // TODO Remove final "*" and use special HashSplitterSearch* analysis and post-process it to create real queries
        // TODO get inspiration from NumericRangeQuery
        return super.rangeFilter(lowerTerm, upperTerm, includeLower, includeUpper, context);
    }
}
