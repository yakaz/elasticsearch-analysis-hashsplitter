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
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@Test
public class WildcardTermEnumTests {

    private Directory index;
    private IndexReader reader;
    private IndexSearcher searcher;

    @BeforeClass
    public void init() throws Exception {
        index = new RAMDirectory();
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
        IndexWriter writer = new IndexWriter(index, cfg);
        writer.addDocument(tokensToDoc("a0000", "b1111", "c2222", "d3333"));
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

    private Document tokensToDoc(String... tokens) {
        Document rtn = new Document();
        rtn.add(new Field("raw", tokensToStream(tokens)));
        return rtn;
    }

    @Test
    public void testExact() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c2222"), '?', '*');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardAnyPrefix() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c2*"), '?', '*');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardAnyPrefixAlternate() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c2%"), '_', '%');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardAnySuffix() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "*2"), '?', '*');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardAnySuffixAlternate() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "%2"), '_', '%');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardOnePrefix() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c2???"), '?', '*');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardOnePrefixAlternate() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c2___"), '_', '%');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardOneSuffix() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "????2"), '?', '*');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testWildcardOneSuffixAlternate() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "____2"), '_', '%');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testCombination() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c?2*"), '?', '*');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

    @Test
    public void testCombinationAlternate() throws Exception {
        WildcardTermEnum termEnum = new WildcardTermEnum(reader, new Term("raw", "c_2%"), '_', '%');
        assertThat(termEnum.term(), not(equalTo(null)));
        assertThat(termEnum.term().field(), equalTo("raw"));
        assertThat(termEnum.term().text(), equalTo("c2222"));
        assertThat(termEnum.next(), equalTo(false));
        assertThat(termEnum.term(), equalTo(null));
        termEnum.close();
    }

}
