package org.apache.lucene.search;

import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A Filter that matches no document.
 * Delegates to {@link DocIdSet.EMPTY_DOCIDSET}.
 */
public class MatchNoDocsFilter extends Filter {

    public static final MatchNoDocsFilter INSTANCE = new MatchNoDocsFilter();

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        return DocIdSet.EMPTY_DOCIDSET;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj instanceof MatchNoDocsFilter;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "MatchNoDocsFilter()";
    }

}
