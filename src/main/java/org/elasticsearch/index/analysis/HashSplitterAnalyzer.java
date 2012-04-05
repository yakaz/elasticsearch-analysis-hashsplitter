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

import org.apache.lucene.analysis.ReusableAnalyzerBase;

import java.io.Reader;

/**
 * Analyzes the input into n-grams of the given size(s).
 */
public class HashSplitterAnalyzer extends ReusableAnalyzerBase {

    public static final int DEFAULT_CHUNK_LENGTH = 1;
    public static final String DEFAULT_PREFIXES  = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789,.";

    public final int chunkLength;
    public final String prefixes;

    public HashSplitterAnalyzer() {
        this(DEFAULT_CHUNK_LENGTH);
    }

    public HashSplitterAnalyzer(int chunkLength) {
        this(chunkLength, DEFAULT_PREFIXES);
    }

    public HashSplitterAnalyzer(int chunkLength, String prefixes) {
        this.chunkLength = chunkLength;
        this.prefixes = prefixes;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader aReader) {
        return new TokenStreamComponents(new HashSplitterTokenizer(aReader, chunkLength, prefixes));
    }

}
