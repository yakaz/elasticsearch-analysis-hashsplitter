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
public class HashSplitterTokenizerTests {

    private HashSplitterTokenizer tokenizer;
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
        tokenizer.close();
        termAttr = null;
        offAttr = null;
        input = null;
    }

    @Test
    public void testDefaultAnalysis() throws Exception {
        tokenizer = new HashSplitterTokenizer(null);

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, tokenizer.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(tokenizer.incrementToken(), equalTo(false));
        tokenizer.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

    @Test
    public void testChunkLength() throws Exception {
        tokenizer = new HashSplitterTokenizer(null, 2, HashSplitterTokenizer.DEFAULT_PREFIXES);

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; i += 2) {
            assertThat("at i = " + i, tokenizer.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i/2,i/2+1) + input.substring(i,i+2)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+2));
        }
        assertThat(tokenizer.incrementToken(), equalTo(false));
        tokenizer.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

    @Test
    public void testIncompleteLastChunk() throws Exception {
        tokenizer = new HashSplitterTokenizer(null, 2, "ab");

        analyze("001");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a00"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(2));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b1"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(2));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(3));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        tokenizer.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }
    
    @Test
    public void testReset() throws Exception {
        tokenizer = new HashSplitterTokenizer(null, 2, "abcd");

        analyze("0011");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a00"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(2));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b11"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(2));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(4));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        tokenizer.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        analyze("2233");
        assertThat("at i = 0", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a22"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(2));
        assertThat("at i = 1", tokenizer.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b33"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(2));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(4));
        assertThat(tokenizer.incrementToken(), equalTo(false));
        tokenizer.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

    @Test
    public void testPrefixes() throws Exception {
        String prefixes = "⁰¹²³⁴⁵⁶⁷⁸⁹";
        assertThat(prefixes.length(), equalTo(10));
        tokenizer = new HashSplitterTokenizer(null, HashSplitterTokenizer.DEFAULT_CHUNK_LENGTH, prefixes);

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, tokenizer.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(prefixes.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(tokenizer.incrementToken(), equalTo(false));
        tokenizer.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

}
