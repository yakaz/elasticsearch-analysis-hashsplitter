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

import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class HashSplitterAnalyzerProviderFactory implements AnalyzerProviderFactory {

    public static class Provider implements AnalyzerProvider<HashSplitterAnalyzer> {

        private final int chunkLength;
        private final String prefixes;

        public Provider(int chunkLength, String prefixes) {
            this.chunkLength = chunkLength;
            this.prefixes = prefixes;
        }

        @Override
        public String name() {
            return "hash_splitter";
        }

        @Override
        public AnalyzerScope scope() {
            return AnalyzerScope.INDEX;
        }

        @Override
        public HashSplitterAnalyzer get() {
            return new HashSplitterAnalyzer(chunkLength, prefixes);
        }

    }

    public static class DefaultProvider extends Provider {

        private static final HashSplitterAnalyzer instance = new HashSplitterAnalyzer();

        public DefaultProvider() {
            super(HashSplitterAnalyzer.DEFAULT_CHUNK_LENGTH, HashSplitterAnalyzer.DEFAULT_PREFIXES);
        }

        @Override
        public AnalyzerScope scope() {
            return AnalyzerScope.GLOBAL;
        }

        @Override
        public HashSplitterAnalyzer get() {
            return instance;
        }

    }

    @Override public AnalyzerProvider create(String name, Settings settings) {
        int chunkLength = settings.getAsInt("chunk_length", HashSplitterAnalyzer.DEFAULT_CHUNK_LENGTH);
        String prefixes = settings.get("prefixes", HashSplitterAnalyzer.DEFAULT_PREFIXES);
        return new Provider(chunkLength, prefixes);
    }

}
