package com.exceeddata.ac.format.dbc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import org.junit.Test;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.exception.EngineException;
import com.exceeddata.ac.common.message.MessageDecoder;
import com.exceeddata.ac.common.message.MessageEncoder;
import com.exceeddata.ac.format.asc.AscMessage;
import com.exceeddata.ac.format.asc.AscMeta;
import com.exceeddata.ac.format.util.MessageDecodeBuilder;
import com.exceeddata.ac.format.util.MessageEncodeBuilder;

public class DbcEncodeDecodeTest {
    
    @Test
    public void testChannelMessageDecodeEncode() throws IOException, EngineException {
        final ClassLoader classLoader = getClass().getClassLoader();
        final URL resource = classLoader.getResource("test.dbc");
        
        final MessageDecoder decoder = MessageDecodeBuilder.buildDBC(resource.getFile(), false, true, true, false, false, null);
        final MessageEncoder encoder = MessageEncodeBuilder.buildDBC(resource.getFile(), true, true, false);
        final ArrayList<String> asc = new ArrayList<>();
        asc.add("0.002095 1  1E5             Tx   d 8 46 FE F6 8F FD 01 0A 00");
        asc.add("0.005103 1  F9              Tx   d 8 00 00 40 00 00 00 00 FF");
        asc.add("0.005339 1  199             Tx   d 8 4F FF 0E 70 F1 8F 00 FF");
        asc.add("0.005515 1  F1              Tx   d 4 00 00 00 40");
        asc.add("0.007130 1  C1              Tx   d 8 30 01 0F CB 30 00 00 00");
        asc.add("0.007380 1  C5              Tx   d 8 30 01 48 7C 30 02 38 CD");
        asc.add("0.007516 1  185             Tx   d 2 00 03");
        asc.add("0.007756 1  1C7             Tx   d 7 06 D6 F9 27 03 FF 3E");
        
        final AscMeta meta = new AscMeta();
        AscMessage message;
        Record record;
        byte[] data;
        for (final String s : asc) {
            message = AscMessage.fromString(s, true, 0);
            record = decoder.decode(meta, message);
            data = encoder.encode(record).get(0).getData();
            assertEquals(data.length, message.getData().length);
            for (int i = 0; i < data.length; ++i) {
                assertEquals(data[i], message.getData()[i]);
            }
        }
    }
    
    @Test
    public void testConsolidateMessageDecodeEncode() throws IOException, EngineException {
        final ClassLoader classLoader = getClass().getClassLoader();
        final URL resource = classLoader.getResource("test.dbc");
        
        final MessageDecoder decoder = MessageDecodeBuilder.buildDBC(resource.getFile(), true, true, true, false, false, null);
        final MessageEncoder encoder = MessageEncodeBuilder.buildDBC(resource.getFile(), true, true, false);
        final ArrayList<String> asc = new ArrayList<>();
        asc.add("0.002095 1  1E5             Tx   d 8 46 FE F6 8F FD 01 0A 00");
        asc.add("0.005103 1  F9              Tx   d 8 00 00 40 00 00 00 00 FF");
        asc.add("0.005339 1  199             Tx   d 8 4F FF 0E 70 F1 8F 00 FF");
        asc.add("0.005515 1  F1              Tx   d 4 00 00 00 40");
        asc.add("0.007130 1  C1              Tx   d 8 30 01 0F CB 30 00 00 00");
        asc.add("0.007380 1  C5              Tx   d 8 30 01 48 7C 30 02 38 CD");
        asc.add("0.007516 1  185             Tx   d 2 00 03");
        asc.add("0.007756 1  1C7             Tx   d 7 06 D6 F9 27 03 FF 3E");
        
        final AscMeta meta = new AscMeta();
        AscMessage message;
        Record record;
        byte[] data;
        for (final String s : asc) {
            message = AscMessage.fromString(s, true, 0);
            record = decoder.decode(meta, message);
            data = encoder.encode(record).get(0).getData();
            assertEquals(data.length, message.getData().length);
            for (int i = 0; i < data.length; ++i) {
                assertEquals(data[i], message.getData()[i]);
            }
        }
    }
}
