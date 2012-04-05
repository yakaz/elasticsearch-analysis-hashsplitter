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


import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.common.io.FastStringReader;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class HashSplitterTokenFilterTests {

    private int chunkLength;
    private String prefixes;

    private KeywordTokenizer tokenizer;
    private HashSplitterTokenFilter tokenFilter;
    private FastStringReader reader;
    private CharTermAttribute termAttr;
    private OffsetAttribute offAttr;
    private String input;

    @BeforeClass
    public void setup() {
        tokenizer = new KeywordTokenizer(null);
    }

    @BeforeMethod
    public void init() {
        chunkLength = HashSplitterTokenFilter.DEFAULT_CHUNK_LENGTH;
        prefixes = HashSplitterTokenFilter.DEFAULT_PREFIXES;
        tokenFilter = null;
        reader = null;
        termAttr = null;
        offAttr = null;
        input = null;
    }

    protected void analyze(String _input) throws Exception {
        input = _input;
        reader = new FastStringReader(input);
        tokenizer.reset(reader);
        tokenFilter = new HashSplitterTokenFilter(tokenizer, chunkLength, prefixes);
        termAttr = tokenFilter.getAttribute(CharTermAttribute.class);
        offAttr = tokenFilter.getAttribute(OffsetAttribute.class);
    }

    protected void closeAnalysis() throws Exception {
        tokenFilter.end();
        tokenFilter.close();
        termAttr = null;
        offAttr = null;
        input = null;
    }

    @Test
    public void testDefaultAnalysis() throws Exception {
        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, tokenFilter.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(tokenFilter.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testChunkLength() throws Exception {
        chunkLength = 2;

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; i += chunkLength) {
            assertThat("at i = " + i, tokenFilter.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i/chunkLength,i/chunkLength+1) + input.substring(i,i+chunkLength)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+2));
        }
        assertThat(tokenFilter.incrementToken(), equalTo(false));
        closeAnalysis();
    }

    @Test
    public void testPrefixes() throws Exception {
        prefixes = "⁰¹²³⁴⁵⁶⁷⁸⁹";
        assertThat(prefixes.length(), equalTo(10));

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, tokenFilter.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(prefixes.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(tokenFilter.incrementToken(), equalTo(false));
        closeAnalysis();
    }

}
