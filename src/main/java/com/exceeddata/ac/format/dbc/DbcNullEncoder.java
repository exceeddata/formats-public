package com.exceeddata.ac.format.dbc;

import java.util.ArrayList;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageEncoder;

public class DbcNullEncoder implements MessageEncoder {
    private static final long serialVersionUID = 1L;
    
    public DbcNullEncoder() {
    }
    
    @Override
    public DbcNullEncoder clone() {
        return new DbcNullEncoder();
    }
    
    public DbcNullEncoder copy() {
        return new DbcNullEncoder();
    }
    
    @Override
    public ArrayList<MessageContent> encode(final Record record) {
        return null;
    }
}
