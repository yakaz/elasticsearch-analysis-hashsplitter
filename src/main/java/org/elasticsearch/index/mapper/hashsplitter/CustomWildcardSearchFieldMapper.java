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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.QueryParseContext;

public interface CustomWildcardSearchFieldMapper {

    /**
     * Return the actual {@link org.apache.lucene.search.Query} to be performed against the current field
     * for the given value.
     * @param value Value to be searched, with possible wildcards.
     * @param method Rewrite method to be used.
     * @param context The current parser context.
     * @return A {@link org.apache.lucene.search.WildcardQuery}, or any other {@link org.apache.lucene.search.Query}
     *         to be used to perform the actual query, or null to use the default, fallback WildcardQuery.
     */
    public Query wildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context);

    /**
     * Return the actual {@link org.apache.lucene.search.Filter} to be performed against the current field
     * for the given value.
     * @param value Value to be searched, with possible wildcards.
     * @param context The current parser context.
     * @return A {@link org.apache.lucene.search.WildcardFilter}, or any other {@link org.apache.lucene.search.Filter}
     *         to be used to perform the actual filter, or null to use the default, fallback WildcardFilter.
     */
    public Filter wildcardFilter(String value, @Nullable QueryParseContext context);

}
