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
 * Analyzes the input to search against HashSplitter analyzed fields.
 */
public class HashSplitterSearchAnalyzer extends ReusableAnalyzerBase {

    public static final int DEFAULT_CHUNK_LENGTH = 1;
    public static final String DEFAULT_PREFIXES  = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789,.";
    public static final char DEFAULT_WILDCARD_ONE = '?';
    public static final char DEFAULT_WILDCARD_ANY = '*';

    public final int chunkLength;
    public final String prefixes;
    public final char wildcardOne;
    public final char wildcardAny;
    public final boolean sizeIsVariable;
    public final int sizeValue;

    public static class Builder {
        private int chunkLength = DEFAULT_CHUNK_LENGTH;
        private String prefixes = DEFAULT_PREFIXES;
        private char wildcardOne = DEFAULT_WILDCARD_ONE;
        private char wildcardAny = DEFAULT_WILDCARD_ANY;
        private boolean sizeIsVariable = true;
        private int sizeValue = -1;

        public Builder setChunkLength(int chunkLength) {
            this.chunkLength = chunkLength;
            return this;
        }

        public Builder setPrefixes(String prefixes) {
            this.prefixes = prefixes;
            return this;
        }

        public Builder setWildcardOne(char wildcardOne) {
            this.wildcardOne = wildcardOne;
            return this;
        }

        public Builder setWildcardAny(char wildcardAny) {
            this.wildcardAny = wildcardAny;
            return this;
        }

        public Builder setSize(Integer size) {
            if (size == null)
                setSizeIsVariable();
            else
                setFixedSize(size.intValue());
            return this;
        }
        
        public Builder setSizeIsVariable() {
            this.sizeIsVariable = true;
            return this;
        }

        public Builder setFixedSize(int sizeValue) {
            this.sizeIsVariable = false;
            this.sizeValue = sizeValue;
            return this;
        }

        public HashSplitterSearchAnalyzer build() {
            return new HashSplitterSearchAnalyzer(chunkLength, prefixes, wildcardOne, wildcardAny, sizeIsVariable, sizeValue);
        }
    }

    public HashSplitterSearchAnalyzer(int chunkLength, String prefixes, char wildcardOne, char wildcardAny, boolean sizeIsVariable, int sizeValue) {
        this.chunkLength = chunkLength;
        this.prefixes = prefixes;
        this.wildcardOne = wildcardOne;
        this.wildcardAny = wildcardAny;
        this.sizeIsVariable = sizeIsVariable;
        this.sizeValue = sizeValue;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader aReader) {
        return new TokenStreamComponents(new HashSplitterSearchTokenizer(aReader, chunkLength, prefixes, wildcardOne, wildcardAny, sizeIsVariable, sizeValue));
    }
    
}
