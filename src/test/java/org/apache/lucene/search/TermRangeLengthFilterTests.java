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

package org.apache.lucene.search;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@Test
public class TermRangeLengthFilterTests {

    private Directory index;
    private IndexReader reader;
    private IndexSearcher searcher;

    @BeforeClass
    public void init() throws Exception {
        index = new RAMDirectory();
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
        IndexWriter writer = new IndexWriter(index, cfg);
        writer.addDocument(tokensToDoc("a", "a"));
        writer.addDocument(tokensToDoc("bb", "bb"));
        writer.addDocument(tokensToDoc("ccc", "ccc"));
        writer.addDocument(tokensToDoc("dddd", "dddd"));
        writer.close();

        reader = IndexReader.open(index, true);
        searcher = new IndexSearcher(reader);
    }

    @AfterClass
    public void tearDown() throws Exception {
        searcher.close();
        reader.close();
        index.close();
    }

    private TokenStream tokensToStream(final String... tokens) {
        return new TokenStream() {
            private int pos = 0;
            private CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
            @Override
            public boolean incrementToken() throws IOException {
                if (pos >= tokens.length)
                    return false;
                termAttr.setEmpty();
                termAttr.append(tokens[pos]);
                pos++;
                return true;
            }
        };
    }

    private Document tokensToDoc(String value, String... tokens) {
        Document rtn = new Document();
        Field field = new Field("raw", false, value, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
        field.setTokenStream(tokensToStream(tokens));
        rtn.add(field);
        return rtn;
    }

    @Test
    public void testCheck() throws Exception {
        Filter filter = new TermRangeFilter("raw", null, null, false, false);

        TopDocs results = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 10);
        assertThat(results.totalHits, equalTo(4));
        Set<String> valuesToBeSeen = new TreeSet<String>(Arrays.asList("a", "bb", "ccc", "dddd"));
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = reader.document(scoreDoc.doc);
            assertThat(doc, not(equalTo(null)));
            String[] values = doc.getValues("raw");
            assertThat(values, not(equalTo(null)));
            assertThat(values.length, equalTo(1));
            assertThat("was an expected value", valuesToBeSeen.remove(values[0]), equalTo(true));
        }
        assertThat("no values left", valuesToBeSeen.isEmpty(), equalTo(true));
    }

    @Test
    public void test() throws Exception {
        Filter filter = new TermRangeLengthFilter("raw", null, null, false, false, 2, 3);

        TopDocs results = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 10);
        assertThat(results.totalHits, equalTo(2));
        Set<String> valuesToBeSeen = new TreeSet<String>(Arrays.asList("bb", "ccc"));
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = reader.document(scoreDoc.doc);
            String[] values = doc.getValues("raw");
            assertThat(values, not(equalTo(null)));
            assertThat(values.length, equalTo(1));
            assertThat("was an expected value", valuesToBeSeen.remove(values[0]), equalTo(true));
        }
        assertThat("no values left", valuesToBeSeen.isEmpty(), equalTo(true));
    }

}
