package org.elasticsearch.index.analysis;

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


import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.common.io.FastStringReader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class HashSplitterSearchTokenizerTests {

    private HashSplitterSearchTokenizer tokenizer;
    private FastStringReader reader;
    private CharTermAttribute termAttr;
    private OffsetAttribute offAttr;
    private String input;

    @BeforeMethod
    public void init() {
        tokenizer = null;
        reader = null;
        termAttr = null;
        offAttr = null;
        input = null;
    }

    protected void analyze(String _input) throws Exception {
        input = _input;
        reader = new FastStringReader(input);
        tokenizer.reset(reader);
        termAttr = tokenizer.getAttribute(CharTermAttribute.class);
        offAttr = tokenizer.getAttribute(OffsetAttribute.class);
    }

    protected void closeAnalysis() throws Exception {
        tokenizer.end();
        tokenizer.close();
        termAttr = null;
        offAttr = null;
        input = null;
    }

    @Test
    public void testDefaultAnalysis() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null);

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, tokenizer.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testChunkLength() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null, 2, HashSplitterSearchTokenizer.DEFAULT_PREFIXES, HashSplitterSearchTokenizer.DEFAULT_WILDCARD_ONE, HashSplitterSearchTokenizer.DEFAULT_WILDCARD_ANY, true, -1);

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; i += 2) {
            assertThat("at i = " + i, tokenizer.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i/2,i/2+1) + input.substring(i,i+2)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+2));
        }
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testPrefixes() throws Exception {
        String prefixes = "⁰¹²³⁴⁵⁶⁷⁸⁹";
        assertThat(prefixes.length(), equalTo(10));
        tokenizer = new HashSplitterSearchTokenizer(null, HashSplitterSearchTokenizer.DEFAULT_CHUNK_LENGTH, prefixes, HashSplitterSearchTokenizer.DEFAULT_WILDCARD_ONE, HashSplitterSearchTokenizer.DEFAULT_WILDCARD_ANY, true, -1);

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, tokenizer.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(prefixes.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testSearchWildcardOne() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null, 4, "abcd", '?', '*', false, 12);

        analyze("00001??12222");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0000"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b1??1"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat("at i = 2", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 2", termAttr.toString(), equalTo("c2222"));
        assertThat("at i = 2", offAttr.startOffset(), equalTo(8));
        assertThat("at i = 2", offAttr.endOffset(), equalTo(12));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();

        analyze("?????11?????");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b?11?"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat("at i = 2", tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testSearchWildcardAnyPrefixVariableSize() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null, 4, "abcd", '?', '*', true, -1);

        analyze("00001*");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0000"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b1???"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();

        analyze("0000111*");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0000"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b111?"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testSearchWildcardAnyPrefixFixedSize() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null, 4, "abcd", '?', '*', false, 12);

        analyze("00001*");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0000"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b1???"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();

        analyze("0000111*");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0000"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b111?"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testSearchWildcardAnySuffixFixedSize() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null, 4, "abcd", '?', '*', false, 12);

        analyze("*12222");
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b???1"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat("at i = 2", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 2", termAttr.toString(), equalTo("c2222"));
        assertThat("at i = 2", offAttr.startOffset(), equalTo(8));
        assertThat("at i = 2", offAttr.endOffset(), equalTo(12));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();

        analyze("*1112222");
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b?111"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat("at i = 2", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 2", termAttr.toString(), equalTo("c2222"));
        assertThat("at i = 2", offAttr.startOffset(), equalTo(8));
        assertThat("at i = 2", offAttr.endOffset(), equalTo(12));
        closeAnalysis();
    }

    @Test
    public void testSearchWildcardAnyPrefixAndSuffixFixedSize() throws Exception {
        tokenizer = new HashSplitterSearchTokenizer(null, 4, "abcd", '?', '*', false, 12);

        analyze("0*12222");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0???"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b???1"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(4));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(8));
        assertThat("at i = 2", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 2", termAttr.toString(), equalTo("c2222"));
        assertThat("at i = 2", offAttr.startOffset(), equalTo(8));
        assertThat("at i = 2", offAttr.endOffset(), equalTo(12));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();

        analyze("0*2");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a0???"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(4));
        assertThat("at i = 2", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 2", termAttr.toString(), equalTo("c???2"));
        assertThat("at i = 2", offAttr.startOffset(), equalTo(8));
        assertThat("at i = 2", offAttr.endOffset(), equalTo(12));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        closeAnalysis();
    }

}
