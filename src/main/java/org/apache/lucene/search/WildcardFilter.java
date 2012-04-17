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

package org.apache.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;


/**
 * @see org.apache.solr.search.WildcardFilter from Solr 3.6
 * @author Modified by ofavre
 * @version $Id: WildcardFilter.java 922957 2010-03-14 20:58:32Z markrmiller $
 */
public class WildcardFilter extends Filter {
    protected final Term term;
    protected final char wildcardOne;
    protected final char wildcardAny;

    public static final char DEFAULT_WILDCARD_ONE = WildcardQuery.DEFAULT_WILDCARD_ONE;
    public static final char DEFAULT_WILDCARD_ANY = WildcardQuery.DEFAULT_WILDCARD_ANY;

    public WildcardFilter(Term wildcardTerm) {
        this(wildcardTerm, DEFAULT_WILDCARD_ONE, DEFAULT_WILDCARD_ANY);
    }

    public WildcardFilter(Term wildcardTerm, char wildcardOne, char wildcardAny) {
        this.term = wildcardTerm;
        this.wildcardOne = wildcardOne;
        this.wildcardAny = wildcardAny;
    }

    public Term getTerm() { return term; }

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
        TermEnum enumerator = new WildcardTermEnum(reader, term, wildcardOne, wildcardAny);
        TermDocs termDocs = reader.termDocs();
        try {
            do {
                Term term = enumerator.term();
                if (term==null) break;
                termDocs.seek(term);
                while (termDocs.next()) {
                    bitSet.set(termDocs.doc());
                }
            } while (enumerator.next());
        } finally {
            termDocs.close();
            enumerator.close();
        }
        return bitSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WildcardFilter that = (WildcardFilter) o;

        if (wildcardAny != that.wildcardAny) return false;
        if (wildcardOne != that.wildcardOne) return false;
        if (term != null ? !term.equals(that.term) : that.term != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = term != null ? term.hashCode() : 0;
        result = 31 * result + (int) wildcardOne;
        result = 31 * result + (int) wildcardAny;
        return result;
    }

    public String toString (String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        if (wildcardOne != DEFAULT_WILDCARD_ONE) {
            buffer.append(',');
            buffer.append(DEFAULT_WILDCARD_ONE);
            buffer.append('=');
            buffer.append(wildcardOne);
        }
        if (wildcardAny != DEFAULT_WILDCARD_ANY) {
            buffer.append(',');
            buffer.append(DEFAULT_WILDCARD_ANY);
            buffer.append('=');
            buffer.append(wildcardAny);
        }
        return buffer.toString();
    }

    @Override
    public String toString () {
        return toString("");
    }

}
