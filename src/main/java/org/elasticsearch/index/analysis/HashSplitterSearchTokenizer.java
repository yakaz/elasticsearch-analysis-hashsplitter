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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tokenizes the input to search against HashSplitter tokenized fields.
 */
public final class HashSplitterSearchTokenizer extends Tokenizer {

  public static final int DEFAULT_CHUNK_LENGTH = HashSplitterSearchAnalyzer.DEFAULT_CHUNK_LENGTH;
  public static final String DEFAULT_PREFIXES  = HashSplitterSearchAnalyzer.DEFAULT_PREFIXES;
  public static final char DEFAULT_WILDCARD_ONE = HashSplitterSearchAnalyzer.DEFAULT_WILDCARD_ONE;
  public static final char DEFAULT_WILDCARD_ANY = HashSplitterSearchAnalyzer.DEFAULT_WILDCARD_ANY;

  private int chunkLength;
  private String prefixes;
  private int prefixCount;
  private char wildcardOne;
  private char wildcardAny;
  private boolean sizeIsVariable;
  private int sizeValue;
  private Matcher wildcardAnySuppresser;
  private String allWildcardOnesChunk;

  private int gramSize;
  private int pos = 0;
  private int prefix = 0;
  private int inLen;
  private String inStr;
  private boolean started = false;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates HashSplitterSearchTokenFilter with given length and prefixes.
   * @param input {@link java.io.Reader} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   * @param wildcardOne the wildcard character that can replace any single other character
   * @param wildcardAny the wildcard character that can replace any sequence of characters.
   *                    It can appear only once in the input.
   *                    If sizeIsVariable, it must be at the end of the input and it generates a prefix query.
   *                    If not, it can be used to express a prefix and/or a suffix query.
   * @param sizeIsVariable whether the hashes have a known fixed length. Prevents suffix queries.
   * @param sizeValue the length of the hashes. This permits anchoring suffix terms.
   */
  public HashSplitterSearchTokenizer(Reader input, int chunkLength, String prefixes, char wildcardOne, char wildcardAny, boolean sizeIsVariable, int sizeValue) {
    super(input);
    init(chunkLength, prefixes, wildcardOne, wildcardAny, sizeIsVariable, sizeValue);
  }

  /**
   * Creates HashSplitterSearchTokenFilter with given length and prefixes.
   * @param source {@link org.apache.lucene.util.AttributeSource} to use
   * @param input {@link java.io.Reader} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   * @param wildcardOne the wildcard character that can replace any single other character
   * @param wildcardAny the wildcard character that can replace any sequence of characters.
   *                    It can appear only once in the input.
   *                    If sizeIsVariable, it must be at the end of the input and it generates a prefix query.
   *                    If not, it can be used to express a prefix and/or a suffix query.
   * @param sizeIsVariable whether the hashes have a known fixed length. Prevents suffix queries.
   * @param sizeValue the length of the hashes. This permits anchoring suffix terms.
   */
  public HashSplitterSearchTokenizer(AttributeSource source, Reader input, int chunkLength, String prefixes, char wildcardOne, char wildcardAny, boolean sizeIsVariable, int sizeValue) {
    super(source, input);
    init(chunkLength, prefixes, wildcardOne, wildcardAny, sizeIsVariable, sizeValue);
  }

  /**
   * Creates HashSplitterSearchTokenFilter with given length and prefixes.
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link java.io.Reader} holding the input to be tokenized
   * @param chunkLength the length of the n-grams to generate (equal to n)
   * @param prefixes the characters to be prepended to each chunks to indicate their position
   * @param wildcardOne the wildcard character that can replace any single other character
   * @param wildcardAny the wildcard character that can replace any sequence of characters.
   *                    It can appear only once in the input.
   *                    If sizeIsVariable, it must be at the end of the input and it generates a prefix query.
   *                    If not, it can be used to express a prefix and/or a suffix query.
   * @param sizeIsVariable whether the hashes have a known fixed length. Prevents suffix queries.
   * @param sizeValue the length of the hashes. This permits anchoring suffix terms.
   */
  public HashSplitterSearchTokenizer(AttributeFactory factory, Reader input, int chunkLength, String prefixes, char wildcardOne, char wildcardAny, boolean sizeIsVariable, int sizeValue) {
    super(factory, input);
    init(chunkLength, prefixes, wildcardOne, wildcardAny, sizeIsVariable, sizeValue);
  }

  /**
   * Creates HashSplitterSearchTokenFilter with default length and prefixes.
   * @param input {@link java.io.Reader} holding the input to be tokenized
   */
  public HashSplitterSearchTokenizer(Reader input) {
    this(input, DEFAULT_CHUNK_LENGTH, DEFAULT_PREFIXES, DEFAULT_WILDCARD_ONE, DEFAULT_WILDCARD_ANY, true, -1);
  }
  
  private void init(int chunkLength, String prefixes, char wildcardOne, char wildcardAny, boolean sizeIsVariable, int sizeValue) {
    if (chunkLength < 1) {
      throw new IllegalArgumentException("chunkLength must be greater than zero");
    }
    if (!sizeIsVariable && sizeValue < 0) {
      throw new IllegalArgumentException("size must be positive");
    }
    this.chunkLength = chunkLength;
    this.prefixes = prefixes;
    this.prefixCount = prefixes.length();
    this.wildcardOne = wildcardOne;
    this.wildcardAny = wildcardAny;
    this.sizeIsVariable = sizeIsVariable;
    this.sizeValue = sizeValue;
    StringBuilder sb = new StringBuilder();
    for (int i = 0 ; i < chunkLength ; ++i)
      sb.append(this.wildcardOne);
    this.allWildcardOnesChunk = sb.toString();
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
      // Check for wildcardAny
      int posFirstAny = inStr.indexOf(wildcardAny);
      if (posFirstAny != -1) {
        if (posFirstAny < inLen - 1 && (sizeIsVariable || inStr.indexOf(wildcardAny, posFirstAny + 1) != -1)) {
          // Invalid case:
          //  - either variable hash size, and "*" not at the end
          //  - or multiple "*"
          // Treat them as matching a 0 length part at least...
          if (wildcardAnySuppresser == null) {
            wildcardAnySuppresser = Pattern.compile(Character.toString(wildcardAny), Pattern.LITERAL).matcher(inStr);
          } else {
            wildcardAnySuppresser.reset(inStr);
          }
          inStr = wildcardAnySuppresser.replaceAll("");
          inLen = inStr.length();
        } else if (posFirstAny == inLen - 1) {
          // Remove final "*"
          inStr = inStr.substring(0, inLen - 1);
          inLen--;
        } else { // We have a single, enclosed "*", and a fixed size
          // Expand the "*" to the right number of "?"s
          StringBuilder sbOnes = new StringBuilder();
          for (int i = sizeValue - inLen + 1 ; i > 0 ; --i)
            sbOnes.append(wildcardOne);
          StringBuilder sbStr = new StringBuilder();
          sbStr.append(inStr, 0, posFirstAny);
          sbStr.append(sbOnes);
          sbStr.append(inStr, posFirstAny+1, inLen);
          inStr = sbStr.toString();
          inLen = sizeValue;
        }
      }
      if (inLen % chunkLength != 0) {
        // Pad the last chunk with "?"s
        StringBuilder sb = new StringBuilder(inLen + chunkLength - (inLen % chunkLength));
        sb.append(inStr);
        for (int i = chunkLength - (inLen % chunkLength) ; i > 0 ; --i)
          sb.append(wildcardOne);
        inStr = sb.toString();
        inLen = inStr.length();
      }
    }

    if (pos >= inLen) {            // if we hit the end of the string
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
    if (inStr.regionMatches(false, oldPos, allWildcardOnesChunk, 0, chunkLength)) {
      // Blank token (all "???"s), skip to the next token
      return incrementToken();
    }
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
    prefix = 0;
  }
}
