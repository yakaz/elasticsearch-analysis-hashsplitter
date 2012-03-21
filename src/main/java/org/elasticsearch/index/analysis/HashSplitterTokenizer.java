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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeSource;

import java.io.IOException;
import java.io.Reader;

/**
 * Tokenizes the input into n-grams of the given size(s).
 */
public final class HashSplitterTokenizer extends Tokenizer {
  public static final int DEFAULT_CHUNK_LENGTH= 1;
  public static final String DEFAULT_PREFIXES = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789,.";

  private int chunkLength;
  private String prefixes;
  private int prefixCount;
  private int gramSize;
  private int pos = 0;
  private int prefix = 0;
  private int inLen;
  private String inStr;
  private boolean started = false;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates HashSplitterTokenFilter with given length and prefixes.
   * @param input {@link Reader} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   */
  public HashSplitterTokenizer(Reader input, int chunkLength, String prefixes) {
    super(input);
    init(chunkLength, prefixes);
  }

  /**
   * Creates HashSplitterTokenFilter with given length and prefixes.
   * @param source {@link AttributeSource} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   */
  public HashSplitterTokenizer(AttributeSource source, Reader input, int chunkLength, String prefixes) {
    super(source, input);
    init(chunkLength, prefixes);
  }

  /**
   * Creates HashSplitterTokenFilter with given length and prefixes.
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   */
  public HashSplitterTokenizer(AttributeFactory factory, Reader input, int chunkLength, String prefixes) {
    super(factory, input);
    init(chunkLength, prefixes);
  }

  /**
   * Creates HashSplitterTokenFilter with default length and prefixes.
   * @param input {@link Reader} holding the input to be tokenized
   */
  public HashSplitterTokenizer(Reader input) {
    this(input, DEFAULT_CHUNK_LENGTH, DEFAULT_PREFIXES);
  }
  
  private void init(int chunkLength, String prefixes) {
    if (chunkLength < 1) {
      throw new IllegalArgumentException("chunkLength must be greater than zero");
    }
    this.chunkLength = chunkLength;
    this.prefixes = prefixes;
    this.prefixCount = prefixes.length();
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    if (!started) {
      started = true;
      char[] chars = new char[1024];
      input.read(chars);
      inStr = new String(chars).trim();  // remove any trailing empty strings 
      inLen = inStr.length();
    }

    if (pos+chunkLength > inLen) {            // if we hit the end of the string
      return false;
    }

    int oldPos = pos;
    pos+=chunkLength;
    gramSize = Math.min(chunkLength, inLen-oldPos);
    termAtt.setEmpty();
    if (prefixCount > 0) termAtt.append(prefixes.charAt(prefix));
    termAtt.append(inStr, oldPos, oldPos+gramSize);
    offsetAtt.setOffset(correctOffset(oldPos), correctOffset(oldPos+gramSize));
    prefix++;
    prefix %= prefixCount;
    return true;
  }
  
  @Override
  public final void end() {
    // set final offset
    final int finalOffset = inLen;
    this.offsetAtt.setOffset(finalOffset, finalOffset);
  }    
  
  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    reset();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    started = false;
    pos = 0;
  }
}
