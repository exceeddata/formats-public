package com.exceeddata.ac.format.csv;

import com.exceeddata.ac.common.util.SparseArray;
import com.exceeddata.ac.common.util.XStringUtils;

/**
 * A sparse text CSV parser class.
 *
 */
public class CsvLineParser {
    private char separatorChar = ',';
    private char quoteChar = '\"';
    private char escapeChar = '\\';
    private char spaceChar = ' ';
    private boolean acceptEscapeChar = false;
    private boolean keepEscapeChar = false;
    private boolean trimWhiteSpaces = true;
    
    public CsvLineParser() {}
    
    /**
     * Construct a <code>CsvLineParser</code> with parameters.
     * 
     * @param separatorChar the separator character
     * @param quoteChar the quote character
     * @param escapeChar the escape character
     * @param spaceChar the space character
     * @param acceptEscapeChar whether to accept the escape character
     */
    public CsvLineParser (
            final char separatorChar, 
            final char quoteChar,
            final char escapeChar,
            final char spaceChar,
            final boolean acceptEscapeChar) {
        this.separatorChar = separatorChar;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.spaceChar = spaceChar;
        this.acceptEscapeChar = acceptEscapeChar;
    }
    
    /**
     * Get quote character.
     * 
     * @return char
     */
    public final char getQuoteChar() { 
        return quoteChar; 
    }
    
    /**
     * Set quote character.
     * 
     * @param quoteChar the quote character
     */
    public final void setQuoteChar(final char quoteChar) { 
        this.quoteChar = quoteChar;
    }
    
    /**
     * Get separator character.
     * 
     * @return char
     */
    public final char getSeparatorChar() { return separatorChar; }
    
    /**
     * Set separator character.
     * 
     * @param separatorChar the separator character
     */
    public final void setSeparatorChar(char separatorChar) { 
        this.separatorChar = separatorChar;
    }
    
    /**
     * Get escape character.
     * 
     * @return char
     */
    public final char getEscapeChar() { return escapeChar; }
    
    /**
     * Set escape character.
     * 
     * @param escapeChar the escape character
     */
    public final void setEscapeChar(char escapeChar) { 
        this.escapeChar = escapeChar;
    }
    
    /**
     * Get space character.
     * 
     * @return char
     */
    public final char getSpaceChar() { return spaceChar; }
    
    /**
     * Set space character.
     * 
     * @param spaceChar the space character
     */
    public final void setSpaceChar(char spaceChar) { 
        this.spaceChar = spaceChar;
    }
    
    /**
     * Get whether to keep escape character, default is false. Set 
     * to true if the string needs to be be parsed again.
     * 
     * @return true or false
     */
    public final boolean getKeepEscapeChar() { return keepEscapeChar; }
    
    /**
     * Set whether to keep escape characters.
     * 
     * @param keepEscapeChar whether to keep the escape character
     */
    public final void setKeepEscapeChar(boolean keepEscapeChar) { 
        this.keepEscapeChar = keepEscapeChar;
    }
    
    /**
     * Get whether to accept escape char, false for csv, default true.
     * 
     * @return true or false
     */
    public final boolean getAcceptEscapeChar() { return acceptEscapeChar; }
    
    /**
     * Set whether to accept escape characters.
     * 
     * @param acceptEscapeChar whether to accept the escape character
     */
    public final void setAcceptEscapeChar(boolean acceptEscapeChar) { 
        this.acceptEscapeChar = acceptEscapeChar;
    }
    
    /**
     * Get whether to trim white space characters when not quoted, default is true. Set 
     * to false if space is meaningful.
     * 
     * @return true or false
     */
    public final boolean getTrimWhiteSpaces() { return trimWhiteSpaces; }
    
    /**
     * Set whether to trim white space characters when not quoted.
     * 
     * @param trimWhiteSpaces whether to trim white spaces
     * @return HDFSSparseTextParser
     */
    public final void setTrimWhiteSpaces(boolean trimWhiteSpaces) { 
        this.trimWhiteSpaces = trimWhiteSpaces;
    }
    

    /**
     * Split a optionally-quoted delimited string into sparse string collection.
     * This method is threadsafe.
     *  
     * @param str delimited string
     * @return SparseStringCollection
     */
    public final SparseArray<String> split (final String str) {
        return splitUnsafe(str, new SparseArray<String>(), new StringBuilder(32));
    }
    /**
     * Split a optionally-quoted delimited string into sparse string collection.
     * This method is the more memory reusable method but not thread-safe.
     *  
     * @param str delimited string
     * @param strings the resulting string array, for performance reason
     * @param builder the string buffer, for performance reason
     * @return SparseStringCollection
     */
    public final SparseArray<String> splitUnsafe (
            final String str, 
            final SparseArray<String> strings,
            final StringBuilder buffer) {
        strings.clear();
        
        if (XStringUtils.isBlank(str)) {
            return strings;
        }
        
        final char[] chars = str.toCharArray();
        final int length = chars.length, lastIndex = length - 1;
        int next = 0, index = 0;
        boolean quoted = false, badquotes = false;
        String piece;
        
        buffer.setLength(0);
        
        for (int i = 0; i < length; ++i) {
            final char c = chars[i];
            if (c == escapeChar) {
                if (acceptEscapeChar && ++i < length) {
                    if (keepEscapeChar) {
                        buffer.append(c);
                    }
                    buffer.append(chars[i]);
                } else {
                    buffer.append(c);
                }
            } else if (c == quoteChar) {
                if (quoted) {
                    if (i+1 < length && chars[i+1] == quoteChar) {
                        //double quote escape, not yet unquote
                        if (keepEscapeChar) {
                            buffer.append(c);
                        }
                        buffer.append(c);
                        ++i;
                    } else {
                        // ready to unquote
                        if ((next = peekSeparator(chars, lastIndex, i+1)) > 0) {
                            buffer.append(c);
                            piece = buffer.toString().trim();
                            if (badquotes == false && piece.charAt(0) == quoteChar) {
                                piece= piece.substring(1, piece.length() - 1);
                            }
                            if (piece.length() != 0) {
                                strings.add(index++, piece);
                            } else {
                                ++index;
                            }
                            buffer.setLength(0);
                            badquotes = false; //clear bad quotes
                            i = next;
                            if ((next = peekConsecutiveSeparator(chars, lastIndex, next + 1)) != i + 1) {
                                index += next - i - 1;
                                i = next - 1;
                            }
                        } else {
                            buffer.append(c);
                            badquotes = true;
                        }
                        quoted = false;
                    }
                } else {
                    buffer.append(c);
                    quoted = true;
                }
            } else if (c==separatorChar) {
                if (quoted) {
                    buffer.append(c);
                } else {
                    piece = trimWhiteSpaces ? buffer.toString().trim() : buffer.toString();
                    if (piece.length() != 0) {
                        strings.add(index++, piece);
                    } else {
                        ++index;
                    }
                    buffer.setLength(0);
                    badquotes = false;
                    if ((next = peekConsecutiveSeparator(chars, lastIndex, i + 1)) != i + 1) {
                        index += next - i - 1;
                        i = next - 1;
                    }
                }
            } else {
                buffer.append(c);
            }
        }
        
        if (buffer.length() != 0) {
            piece = trimWhiteSpaces ? buffer.toString().trim() : buffer.toString();
            if (piece.length() != 0) {
                strings.add(index++, piece);
            } else {
                ++index;
            }
        } else if (chars[length - 1] == separatorChar) {
            ++index; //protect against null last entry
        }

        strings.setSize(index);
        return strings;
    }
    
    private int peekSeparator(final char[] chars, final int lastIndex, final int index) {
        if (index > lastIndex) {
            return index;  //already at end.
        }
        int offset = index;
        char c = chars[offset];
        while (c != separatorChar && c == spaceChar && offset != lastIndex) {
            c = chars[++offset];
        }
        
        if (offset > lastIndex || c == separatorChar) { //end or finding separator
            return offset;
        } else {
            return -1;
        }
    }
    
    private int peekConsecutiveSeparator(final char[] chars, final int lastIndex, final int index) {
        if (index > lastIndex) {
            return index;  //already at end.
        }
        int offset = index;
        char c = chars[offset];
        
        while (c == separatorChar && offset != lastIndex) {
            c = chars[++offset];
        }
        
        return offset;
    }
}
