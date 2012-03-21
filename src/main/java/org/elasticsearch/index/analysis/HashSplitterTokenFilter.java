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

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Tokenizes the input into n-grams of the given size(s).
 */
public final class HashSplitterTokenFilter extends TokenFilter {
  public static final int DEFAULT_CHUNK_LENGTH= 1;
  public static final String DEFAULT_PREFIXES = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789,.";

  private int chunkLength;
  private String prefixes;
  private int prefixCount;
  
  private char[] curTermBuffer;
  private int curTermLength;
  private int curGramSize;
  private int curPos;
  private int curPrefix;
  private int tokStart;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates HashSplitterTokenFilter with given length and prefixes.
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   */
  public HashSplitterTokenFilter(TokenStream input, int chunkLength, String prefixes) {
    super(input);
    if (chunkLength < 1) {
      throw new IllegalArgumentException("chunkLength must be greater than zero");
    }
    this.chunkLength = chunkLength;
    this.prefixes = prefixes;
    this.prefixCount = prefixes.length();
  }

  /**
   * Creates HashSplitterTokenFilter with default length and prefixes.
   * @param input {@link TokenStream} holding the input to be tokenized
   */
  public HashSplitterTokenFilter(TokenStream input) {
    this(input, DEFAULT_CHUNK_LENGTH, DEFAULT_PREFIXES);
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public final boolean incrementToken() throws IOException {
    if (curTermBuffer == null) {
      if (!input.incrementToken()) {
        return false;
      } else {
        curTermBuffer = termAtt.buffer().clone();
        curTermLength = termAtt.length();
        curPos = 0;
        curPrefix = 0;
        tokStart = offsetAtt.startOffset();
      }
    }
    while (curPos < curTermLength) {     // while there is input
      clearAttributes();
      curGramSize = Math.min(chunkLength, curTermLength-curPos);
      if (prefixCount > 0) {
        termAtt.setEmpty();
        char[] buffer = termAtt.resizeBuffer(1+curGramSize);
        buffer[0] = prefixes.charAt(curPrefix);
        System.arraycopy(buffer, 1, curTermBuffer, curPos, curGramSize);
        termAtt.setLength(1+curGramSize);
      } else {
        termAtt.copyBuffer(curTermBuffer, curPos, curGramSize);
      }
      offsetAtt.setOffset(tokStart + curPos, tokStart + curPos + curGramSize);
      curPos += chunkLength;
      curPrefix++;
      curPrefix %= prefixCount;
      return true;
    }
    return false;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    curTermBuffer = null;
  }
}
