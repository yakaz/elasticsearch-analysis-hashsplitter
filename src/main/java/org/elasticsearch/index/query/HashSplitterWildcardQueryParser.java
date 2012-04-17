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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.hashsplitter.CustomWildcardSearchFieldMapper;
import org.elasticsearch.index.mapper.hashsplitter.HashSplitterFieldMapper;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

/**
 *
 */
public class HashSplitterWildcardQueryParser implements QueryParser {

    public static final String NAME = "hashsplitter_wildcard";

    @Inject
    public HashSplitterWildcardQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "["+NAME+"] query malformed, no field");
        }
        String fieldName = parser.currentName();
        String rewriteMethod = null;

        String value = null;
        float boost = 1.0f;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("wildcard".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("rewrite".equals(currentFieldName)) {
                        rewriteMethod = parser.textOrNull();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "["+NAME+"] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            parser.nextToken();
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for "+NAME+" query");
        }

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            FieldMapper mapper = smartNameFieldMappers.mapper();
            if (mapper != null && mapper instanceof CustomWildcardSearchFieldMapper) {
                CustomWildcardSearchFieldMapper hashsplitterMapper = (CustomWildcardSearchFieldMapper) mapper;
                query = hashsplitterMapper.wildcardQuery(value, QueryParsers.parseRewriteMethod(rewriteMethod), parseContext);
            }
            if (query == null) { // not associated with a HashSplitterFieldMapper OR wildcardQuery() returned null
                // Fallback on the same code as org.elasticsearch.index.query.WildcardQueryParser
                fieldName = smartNameFieldMappers.mapper().names().indexName();
                value = smartNameFieldMappers.mapper().indexedValue(value);
            }
        }
        if (query == null) {
            WildcardQuery q = new WildcardQuery(new Term(fieldName, value));
            q.setRewriteMethod(QueryParsers.parseRewriteMethod(rewriteMethod));
            query = q;
        }
        query.setBoost(boost);
        return wrapSmartNameQuery(query, smartNameFieldMappers, parseContext);
    }
}
