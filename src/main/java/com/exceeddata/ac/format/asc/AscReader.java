package com.exceeddata.ac.format.asc;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

import com.exceeddata.ac.common.extern.ExternMessageReader;
import com.exceeddata.ac.common.util.SpaceSimpleParser;

public class AscReader implements ExternMessageReader, Serializable {
    private static final long serialVersionUID = 1L;

    private Reader reader = null;
    private AscMeta meta = null;
    private InputStream istream = null;

    private StringBuilder row = new StringBuilder(4096);
    private int previousChar = -1;
    private String charset = "UTF-8";
    private AscMessage message = null;
    private long lastOffset = 0;
    
    public AscReader(final String path) throws IOException {
        meta = new AscMeta();
        istream = new BufferedInputStream(Files.newInputStream(Paths.get(path), StandardOpenOption.READ));
        reader = new InputStreamReader(istream, charset);
    }
    
    public AscReader(final InputStream stream) throws IOException {
        meta = new AscMeta();
        this.istream = stream;
        this.reader = new InputStreamReader(istream, charset);
    }
    
    @Override
    public AscMeta meta() {
        return meta;
    }
    
    @Override
    public AscMessage get() {
        return message;
    }
    
    @Override
    public boolean next() throws IOException {
        String line = null;
        char c;
        
        while (nextRow()) {
            line = row.toString().trim().toLowerCase();
            if (line.length() == 0) {
                continue;
            }
            
            c = line.charAt(0);
            if (c >= '0' && c <= '9') {
                message = AscMessage.fromString(line, meta.getHexBase(), lastOffset);
                if (message != null) {
                    if (meta.getRelative()) {
                        lastOffset = message.getNanosOffset();
                    }
                    return true;
                }
            } else if (c == 'd') {
                if (line.startsWith("date ")) {
                    meta.setTimeStart(Instant.ofEpochMilli(AscUtils.parseASCDateTime(line)));
                }
            } else if (c == 'b') {
                if (line.startsWith("begin triggerblock ")) {
                    meta.setTimeStart(Instant.ofEpochMilli(AscUtils.parseASCDateTime(line)));
                } else if (line.startsWith("base ")) {
                    final List<String> pieces = SpaceSimpleParser.split(line);
                    meta.setHexBase(true);
                    meta.setRelative(pieces.size() > 3 && "relative".equalsIgnoreCase(pieces.get(3)));
                }
            }
        }
        
        return false;
    }
    
    private boolean nextRow() throws IOException {
        boolean quoted = false;
        int i, numRead = 0;
        
        //clear previous
        row.setLength(0);
        if (previousChar != -1) {
            ++numRead;
            row.append((char) previousChar);
            if (previousChar == '"') {
                quoted = true;
            }
            previousChar = -1;
        }
        
        while ((i = reader.read()) != -1) {
            ++numRead;
            if (quoted) {
                row.append((char) i);
                if (i == '"') {
                    //peek next to see if it is escaped double ""
                    if ((i = reader.read()) == -1) {
                        break; //EOF
                    } else if (i == '"') {
                        row.append(i);
                    } else {
                        quoted = false;
                    }
                }
            } else if (i == '\n') {
                break;
            } else if (i == '\r') {
                //peek next to see if it is \r\n
                if ((previousChar = reader.read()) != -1) {
                    if (previousChar == '\n') {
                        previousChar = -1; //reset previous char
                    }
                }
                break;
            } else {
                row.append((char) i);
                if (i == '"') {
                    quoted = true;
                }
            }
        }
        
        return numRead > 0;
    }
    
    @Override
    public void close() {
        if (reader != null) {
            try { reader.close(); } catch (IOException e) {}
            reader = null;
        }
        if (istream != null) {
            try { istream.close(); } catch (IOException e) {}
            istream = null;
        }
    }
}
