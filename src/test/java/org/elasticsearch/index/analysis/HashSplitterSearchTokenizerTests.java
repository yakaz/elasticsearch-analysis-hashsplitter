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

    // TODO testSearchWildcardOne
    // TODO testSearchWildcardAny prefix variable size
    // TODO testSearchWildcardAny prefix fixed size
    // TODO testSearchWildcardAny suffix fixed size
    // TODO testSearchWildcardAny prefix and suffix fixed size

}
