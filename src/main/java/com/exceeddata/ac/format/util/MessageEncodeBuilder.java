package com.exceeddata.ac.format.util;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.exceeddata.ac.common.exception.EngineException;
import com.exceeddata.ac.common.message.MessageEncoder;
import com.exceeddata.ac.common.util.FileOperationUtils;
import com.exceeddata.ac.common.util.XStringUtils;
import com.exceeddata.ac.format.dbc.DbcChannel;
import com.exceeddata.ac.format.dbc.DbcDelegator;
import com.exceeddata.ac.format.dbc.DbcDelegatorMessage;
import com.exceeddata.ac.format.dbc.DbcMessage;
import com.exceeddata.ac.format.dbc.DbcMessageEncoder;
import com.exceeddata.ac.format.dbc.DbcNullEncoder;

/**
 * A util class for building inspector from DBC file paths.
 *
 */
public final class MessageEncodeBuilder {
    private MessageEncodeBuilder(){}
    
    public static MessageEncoder buildDBC(
            final String dbcPaths,
            final boolean useQualifiedName,
            final boolean applyFormula,
            final boolean deduplicateMessage) throws EngineException {
        if (XStringUtils.isBlank(dbcPaths)) {
            return new DbcNullEncoder();
        }
        
        final Set<Long> messageids = new HashSet<>(); 
        final DbcMessageEncoder encoder = new DbcMessageEncoder();
        final String[] paths = dbcPaths.split(",");
        final int size = paths.length;
        DbcDelegator delegator;
        DbcChannel channel;
        List<String> lines;
        String line;
        int channelID;
        
        for (int i = 0; i < size; ++i) {
            channelID = i + 1;
            channel = new DbcChannel();
            channel.setChannelID(channelID);
            
            if (XStringUtils.isNotBlank(paths[i])) {
                delegator = null;
                lines = FileOperationUtils.readFileToList(paths[i], false);
                for (int j = 0, s = lines.size(); j < s; ++j) {
                    line = lines.get(j).trim();
                    if (XStringUtils.isNotBlank(line)) {
                        if (DbcDelegatorMessage.matchesMessage(line)) {
                            delegator = new DbcDelegatorMessage(channelID, line); //new delegator
                        } else if (DbcDelegatorMessage.matchesAttribute(line)) {
                            if (delegator == null) {
                                throw new RuntimeException("FORMAT_DBC_ATTRIBUTE_UNEXPECTED: " + line);
                            }
                            delegator.delegate(channel, line, useQualifiedName, applyFormula);
                        } else {
                            delegator = null;
                        }
                    }
                }
            }
            
            //post-processing, remove invalid message.
            final LinkedHashMap<Long, DbcMessage> messages = new LinkedHashMap<>();
            for (final Map.Entry<Long, DbcMessage> entry : channel.getMessages().entrySet()) {
                if (entry.getValue().size() > 0 && (!deduplicateMessage || !messageids.contains(entry.getKey()))) {
                    messages.put(entry.getKey(), entry.getValue());
                    messageids.add(entry.getKey());
                }
            }
         
            for (final DbcMessage message : messages.values()) {
                encoder.addMessage(message);
            }
        }
        
        return encoder;
    }
    
    public static MessageEncoder buildDBCFromFileContents(
            final List<String> dbcFileContents,
            final boolean useQualifiedName,
            final boolean applyFormula,
            final boolean deduplicateMessage) throws EngineException {
        if (dbcFileContents == null || dbcFileContents.size() == 0) {
            return new DbcNullEncoder();
        }
        
        final Set<Long> messageids = new HashSet<>(); 
        final DbcMessageEncoder encoder = new DbcMessageEncoder();
        final int size = dbcFileContents.size();
        DbcDelegator delegator;
        DbcChannel channel;
        String[] lines;
        String line;
        int channelID;
        
        for (int i = 0; i < size; ++i) {
            channelID = i + 1;
            channel = new DbcChannel();
            channel.setChannelID(channelID);
            
            if (XStringUtils.isNotBlank(dbcFileContents.get(i))) {
                delegator = null;
                lines = dbcFileContents.get(i).split(Pattern.quote("\n"));
                for (int j = 0, s = lines.length; j < s; ++j) {
                    line = lines[j].trim();
                    if (XStringUtils.isNotBlank(line)) {
                        if (DbcDelegatorMessage.matchesMessage(line)) {
                            delegator = new DbcDelegatorMessage(channelID, line); //new delegator
                        } else if (DbcDelegatorMessage.matchesAttribute(line)) {
                            if (delegator == null) {
                                throw new RuntimeException("FORMAT_DBC_ATTRIBUTE_UNEXPECTED: " + line);
                            }
                            delegator.delegate(channel, line, useQualifiedName, applyFormula);
                        } else {
                            delegator = null;
                        }
                    }
                }
            }
            
            //post-processing, remove invalid message.
            final LinkedHashMap<Long, DbcMessage> messages = new LinkedHashMap<>();
            for (final Map.Entry<Long, DbcMessage> entry : channel.getMessages().entrySet()) {
                if (entry.getValue().size() > 0 && (!deduplicateMessage || !messageids.contains(entry.getKey()))) {
                    messages.put(entry.getKey(), entry.getValue());
                    messageids.add(entry.getKey());
                }
            }
         
            for (final DbcMessage message : messages.values()) {
                encoder.addMessage(message);
            }
        }
        
        return encoder;
    }
}
