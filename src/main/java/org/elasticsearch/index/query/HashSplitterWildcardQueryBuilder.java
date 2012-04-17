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

import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Implements the wildcard search query.
 * Note this query can be a bit slow, as it needs to iterate over a number of terms.
 */
public class HashSplitterWildcardQueryBuilder extends BaseQueryBuilder {

    private final String name;

    private final String wildcard;

    private float boost = -1;

    private String rewrite;

    public static HashSplitterWildcardQueryBuilder hashSplitterWildcardQuery(String name, String value) {
        return new HashSplitterWildcardQueryBuilder(name, value);
    }

    /**
     * Implements the wildcard search query.
     * Note this query can be a bit slow, as it needs to iterate over a number of terms.
     *
     * @param name     The field name
     * @param wildcard The wildcard query string
     */
    public HashSplitterWildcardQueryBuilder(String name, String wildcard) {
        this.name = name;
        this.wildcard = wildcard;
    }

    public HashSplitterWildcardQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public HashSplitterWildcardQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HashSplitterWildcardQueryParser.NAME);
        if (boost == -1 && rewrite == null) {
            builder.field(name, wildcard);
        } else {
            builder.startObject(name);
            builder.field("wildcard", wildcard);
            if (boost != -1) {
                builder.field("boost", boost);
            }
            if (rewrite != null) {
                builder.field("rewrite", rewrite);
            }
            builder.endObject();
        }
        builder.endObject();
    }
}
