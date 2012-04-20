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


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.common.io.FastStringReader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class HashSplitterSearchAnalyzerTests {

    private HashSplitterSearchAnalyzer.Builder builder;
    private HashSplitterSearchAnalyzer analyzer;
    private FastStringReader reader;
    private TokenStream stream;
    private CharTermAttribute termAttr;
    private OffsetAttribute offAttr;
    private String input;
    
    @BeforeMethod
    public void newBuilder() {
        builder = new HashSplitterSearchAnalyzer.Builder();
        analyzer = null;
        reader = null;
        stream = null;
        termAttr = null;
        offAttr = null;
        input = null;
    }
    
    protected void build() throws Exception {
        analyzer = builder.build();
        reader = null;
        stream = null;
        termAttr = null;
        offAttr = null;
        input = null;
    }

    protected void analyze(String _input) throws Exception {
        input = _input;
        reader = new FastStringReader(input);
        stream = analyzer.reusableTokenStream("hash", reader);
        termAttr = stream.getAttribute(CharTermAttribute.class);
        offAttr = stream.getAttribute(OffsetAttribute.class);
    }

    protected void closeAnalysis() throws Exception {
        stream.end();
        stream.close();
        termAttr = null;
        offAttr = null;
        input = null;
    }

    @Test
    public void testDefaultAnalysis() throws Exception {
        build();

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, stream.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(stream.incrementToken(), equalTo(false));
        closeAnalysis();
    }
    
    @Test
    public void testChunkLength() throws Exception {
        builder.setChunkLength(2);
        build();

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; i += 2) {
            assertThat("at i = " + i, stream.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(HashSplitterSearchAnalyzer.DEFAULT_PREFIXES.substring(i/2,i/2+1) + input.substring(i,i+2)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+2));
        }
        assertThat(stream.incrementToken(), equalTo(false));
        closeAnalysis();
    }
    
    @Test
    public void testPrefixes() throws Exception {
        String prefixes = "⁰¹²³⁴⁵⁶⁷⁸⁹";
        assertThat(prefixes.length(), equalTo(10));
        builder.setPrefixes(prefixes);
        build();

        analyze("0123456789");
        for (int i = 0 ; i < input.length() ; ++i) {
            assertThat("at i = " + i, stream.incrementToken(), equalTo(true));
            assertThat("at i = " + i, termAttr.toString(), equalTo(prefixes.substring(i,i+1) + input.substring(i,i+1)));
            assertThat("at i = " + i, offAttr.startOffset(), equalTo(i));
            assertThat("at i = " + i, offAttr.endOffset(), equalTo(i+1));
        }
        assertThat(stream.incrementToken(), equalTo(false));
        closeAnalysis();
    }

}
