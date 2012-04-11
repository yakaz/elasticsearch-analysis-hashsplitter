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

    private boolean reuse;
    private int chunkLength;
    private String prefixes;

    private HashSplitterTokenizer stream;
    private FastStringReader reader;
    private CharTermAttribute termAttr;
    private OffsetAttribute offAttr;
    private String input;

    @BeforeMethod
    public void init() {
        reuse = false;
        chunkLength = HashSplitterTokenFilter.DEFAULT_CHUNK_LENGTH;
        prefixes = HashSplitterTokenFilter.DEFAULT_PREFIXES;
        stream = null;
        reader = null;
        termAttr = null;
        offAttr = null;
        input = null;
    }
    
    protected void analyze(String _input) throws Exception {
        input = _input;
        reader = new FastStringReader(input);
        if (!reuse || stream == null)
            stream = new HashSplitterTokenizer(reader, chunkLength, prefixes);
        else
            stream.reset(reader);
        termAttr = stream.getAttribute(CharTermAttribute.class);
        offAttr = stream.getAttribute(OffsetAttribute.class);
    }

    protected void closeAnalysis() throws Exception {
        stream.close();
        termAttr = null;
        offAttr = null;
        input = null;
    }

    @Test
    public void testDefaultAnalysis() throws Exception {
        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, stream.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

    @Test
    public void testChunkLength() throws Exception {
        chunkLength = 2;

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; i += chunkLength) {
            assertThat("at i = " + i, stream.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i/chunkLength,i/chunkLength+1) + input.substring(i,i+chunkLength)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+2));
        }
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

    @Test
    public void testPrefixes() throws Exception {
        prefixes = "⁰¹²³⁴⁵⁶⁷⁸⁹";
        assertThat(prefixes.length(), equalTo(10));

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, stream.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(prefixes.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

    @Test
    public void testIncompleteLastChunk() throws Exception {
        chunkLength = 2;
        prefixes = "ab";

        analyze("001");
        assertThat("at i = 0", stream.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a00"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(2));
        assertThat("at i = 1", stream.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b1"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(2));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(3));
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }
    
    @Test
    public void testReset() throws Exception {
        chunkLength = 2;
        prefixes = "abcd";

        analyze("0011");
        assertThat("at i = 0", stream.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a00"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(2));
        assertThat("at i = 1", stream.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b11"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(2));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(4));
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        reuse = true;

        analyze("2233");
        assertThat("at i = 0", stream.incrementToken(), equalTo(true));
        assertThat("at i = 0", termAttr.toString(), equalTo("a22"));
        assertThat("at i = 0", offAttr.startOffset(), equalTo(0));
        assertThat("at i = 0", offAttr.endOffset(), equalTo(2));
        assertThat("at i = 1", stream.incrementToken(), equalTo(true));
        assertThat("at i = 1", termAttr.toString(), equalTo("b33"));
        assertThat("at i = 1", offAttr.startOffset(), equalTo(2));
        assertThat("at i = 1", offAttr.endOffset(), equalTo(4));
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        assertThat("final offset start", offAttr.startOffset(), equalTo(input.length()));
        assertThat("final offset end", offAttr.endOffset(), equalTo(input.length()));

        closeAnalysis();
    }

}
