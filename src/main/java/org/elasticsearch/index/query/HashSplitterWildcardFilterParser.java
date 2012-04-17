/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.WildcardFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.hashsplitter.CustomWildcardSearchFieldMapper;
import org.elasticsearch.index.mapper.hashsplitter.HashSplitterFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 *
 */
public class HashSplitterWildcardFilterParser implements FilterParser {

    public static final String NAME = "hashsplitter_wildcard";

    @Inject
    public HashSplitterWildcardFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean cache = false;
        CacheKeyFilter.Key cacheKey = null;
        String filterName = null;
        String value = null;

        String fieldName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                } else {
                    fieldName = currentFieldName;
                    value = parser.text();
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "No field specified for term filter");
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for "+NAME+" query");
        }

        Filter filter = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            String[] previousTypes = null;
            try {
                if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                    previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{smartNameFieldMappers.docMapper().type()});
                }
                FieldMapper mapper = smartNameFieldMappers.mapper();
                if (mapper != null && mapper instanceof CustomWildcardSearchFieldMapper) {
                    CustomWildcardSearchFieldMapper hashsplitterMapper = (CustomWildcardSearchFieldMapper) mapper;
                    filter = hashsplitterMapper.wildcardFilter(value, parseContext);
                    if (filter == null) {
                        // No useful wildcardFilter() implementation, try wildcardQuery()
                        Query query = hashsplitterMapper.wildcardQuery(value, MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE, parseContext);
                        if (query != null)
                            filter = new QueryWrapperFilter(query);
                    }
                }
                if (filter == null) { // not associated with a HashSplitterFieldMapper OR wildcardFilter/Query() returned null
                    // Fallback on the same code as org.elasticsearch.index.query.WildcardQueryParser
                    fieldName = smartNameFieldMappers.mapper().names().indexName();
                    value = smartNameFieldMappers.mapper().indexedValue(value);
                }
            } finally {
                if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                    QueryParseContext.setTypes(previousTypes);
                }
            }
        }
        if (filter == null) {
            WildcardFilter f = new WildcardFilter(new Term(fieldName, value));
            filter = f;
        }

        if (cache) {
            filter = parseContext.cacheFilter(filter, cacheKey);
        }
        filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }

        return filter;
    }
}
