package com.exceeddata.ac.format.dbc;

import static com.exceeddata.ac.common.message.MessageConstants.CHANNEL;
import static com.exceeddata.ac.common.message.MessageConstants.CHANNEL_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.CONTENT;
import static com.exceeddata.ac.common.message.MessageConstants.CONTENT_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.DIRECTION;
import static com.exceeddata.ac.common.message.MessageConstants.DIRECTION_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.ERROR;
import static com.exceeddata.ac.common.message.MessageConstants.ERROR_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.ID;
import static com.exceeddata.ac.common.message.MessageConstants.ID_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.OFFSET;
import static com.exceeddata.ac.common.message.MessageConstants.OFFSET_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.TIME;
import static com.exceeddata.ac.common.message.MessageConstants.TIME_HASH;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.data.record.RecordBuilder;
import com.exceeddata.ac.common.data.typedata.BinaryData;
import com.exceeddata.ac.common.data.typedata.BooleanData;
import com.exceeddata.ac.common.data.typedata.InstantData;
import com.exceeddata.ac.common.data.typedata.IntData;
import com.exceeddata.ac.common.data.typedata.LongData;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDecoder;
import com.exceeddata.ac.common.message.MessageDesc;
import com.exceeddata.ac.common.message.MessageDirection;

public class DbcNullDecoder implements MessageDecoder {
    private static final long serialVersionUID = 1L;
    private static final Record TEMPLATE = RecordBuilder.newTemplateRecord(new String[] {TIME, OFFSET, CHANNEL, ERROR, DIRECTION, ID, CONTENT});
    
    public DbcNullDecoder() {
    }
    
    @Override
    public boolean getOutputOffset() {
        return false;
    }
    
    @Override
    public DbcNullDecoder clone() {
        return new DbcNullDecoder();
    }
    
    public DbcNullDecoder copy() {
        return new DbcNullDecoder();
    }
    
    @Override
    public Record compute(final MessageDesc desc, final MessageContent message, final boolean applyFormula) {
        return decode(desc, message); //same implementation for decode & interpret
    }
    
    @Override
    public Record compute(
            final MessageDesc desc, 
            final MessageContent message,
            final Record target,
            final boolean applyFormula) {
        return decode(desc, message, target); //same implementation for decode & interpret
    }
    
    @Override
    public Record decode(final MessageDesc desc, final MessageContent message) {
        final Instant start = desc.getTimeStart();
        return TEMPLATE.unsafeNoDataCopy()
                .setAt(0, start != null ? new InstantData(start.plusNanos(message.getNanosOffset())) : InstantData.NULL)
                .setAt(1, new LongData(message.getNanosOffset()))
                .setAt(2, IntData.nonNullValueOf(message.getChannelID()))
                .setAt(3, message.isError() ? BooleanData.TRUE : BooleanData.FALSE)
                .setAt(4, message.getDirection() == MessageDirection.TX ? DbcEnums.TX : DbcEnums.RX)
                .setAt(5, LongData.nonNullValueOf(message.getMessageID()))
                .setAt(6, BinaryData.valueOf(
                        message.getDataLength() == message.getData().length ? message.getData() : Arrays.copyOf(message.getData(), message.getDataLength())
                    ));
    }
    
    @Override
    public Record decode(
            final MessageDesc desc, 
            final MessageContent message, 
            final Record target) {
        final Instant start = desc.getTimeStart();
        return target.add(TIME, TIME_HASH, start != null ? new InstantData(start.plusNanos(message.getNanosOffset())) : InstantData.NULL)
                 .add(OFFSET, OFFSET_HASH, new LongData(message.getNanosOffset()))
                 .add(CHANNEL, CHANNEL_HASH, IntData.valueOf(message.getChannelID()))
                 .add(ERROR, ERROR_HASH, message.isError() ? BooleanData.TRUE : BooleanData.FALSE)
                 .add(DIRECTION, DIRECTION_HASH, message.getDirection() == MessageDirection.TX ? DbcEnums.TX : DbcEnums.RX)
                 .add(ID, ID_HASH, LongData.valueOf(message.getMessageID()))
                 .add(CONTENT, CONTENT_HASH, BinaryData.valueOf(
                        message.getDataLength() == message.getData().length ? message.getData() : Arrays.copyOf(message.getData(), message.getDataLength())
                    ));
    }
    
    @Override
    public Record interpret(final MessageDesc desc, final MessageContent message) {
        return decode(desc, message); //same as decode because cannot be interpreted
    }
    
    @Override
    public Record interpret(
            final MessageDesc desc, 
            final MessageContent message, 
            final Record target) {
        return decode(desc, message, target);
    }
    
    @Override
    public void select(final Set<String> selectedAttributes) {
        //do nothing since binary messages have no names
    }
}
