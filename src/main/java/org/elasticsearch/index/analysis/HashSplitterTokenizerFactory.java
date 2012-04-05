/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;


/**
 * Uses the {@link HashSplitterTokenizer} to split tokens.
 *
 * <p>
 *   The setting <tt>chunk</tt> changes the length of n-grams to genereate.<br/>
 *   The setting <tt>prefixes</tt> changes the characters to prepend,
 *   in order to distinguish the original position of the generated tokens.
 * </p>
 *
 * @author ofavre
 */
public class HashSplitterTokenizerFactory extends AbstractTokenizerFactory {

    private final int chunkLength;
    private final String prefixes;

    @Inject public HashSplitterTokenizerFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.chunkLength = settings.getAsInt("chunk_length", HashSplitterTokenizer.DEFAULT_CHUNK_LENGTH);
        this.prefixes = settings.get("prefixes", HashSplitterTokenizer.DEFAULT_PREFIXES);
    }

    @Override public Tokenizer create(Reader reader) {
        return new HashSplitterTokenizer(reader, chunkLength, prefixes);
    }

}
