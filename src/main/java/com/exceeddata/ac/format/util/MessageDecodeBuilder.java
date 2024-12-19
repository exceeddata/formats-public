package com.exceeddata.ac.format.util;

import static com.exceeddata.ac.common.message.MessageConstants.OFFSET;
import static com.exceeddata.ac.common.message.MessageConstants.TIME;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.data.template.Desc;
import com.exceeddata.ac.common.data.template.PrimitiveDescType;
import com.exceeddata.ac.common.data.template.Template;
import com.exceeddata.ac.common.data.type.Types;
import com.exceeddata.ac.common.data.typedata.DecimalData;
import com.exceeddata.ac.common.data.typedata.DoubleData;
import com.exceeddata.ac.common.data.typedata.InstantData;
import com.exceeddata.ac.common.data.typedata.IntData;
import com.exceeddata.ac.common.data.typedata.LongData;
import com.exceeddata.ac.common.exception.EngineException;
import com.exceeddata.ac.common.message.MessageDecoder;
import com.exceeddata.ac.common.util.FileOperationUtils;
import com.exceeddata.ac.common.util.XStringUtils;
import com.exceeddata.ac.format.dbc.DbcAttribute;
import com.exceeddata.ac.format.dbc.DbcAttributeDecimal;
import com.exceeddata.ac.format.dbc.DbcAttributeDouble;
import com.exceeddata.ac.format.dbc.DbcAttributeInteger;
import com.exceeddata.ac.format.dbc.DbcAttributeLong;
import com.exceeddata.ac.format.dbc.DbcChannel;
import com.exceeddata.ac.format.dbc.DbcChannelMessageDecoder;
import com.exceeddata.ac.format.dbc.DbcConsolidateMessageDecoder;
import com.exceeddata.ac.format.dbc.DbcDelegator;
import com.exceeddata.ac.format.dbc.DbcDelegatorMessage;
import com.exceeddata.ac.format.dbc.DbcMessage;
import com.exceeddata.ac.format.dbc.DbcNullDecoder;

/**
 * A util class for building inspector from DBC file paths.
 *
 */
public final class MessageDecodeBuilder {
    private MessageDecodeBuilder(){}
    
    public static MessageDecoder buildDBC(
            final String dbcPaths,
            final boolean consolidateSchema,
            final boolean useQualifiedName,
            final boolean applyFormula,
            final boolean deduplicateMessage,
            final boolean outputOffset,
            final Set<String> selectedAttributes) throws EngineException {
        if (XStringUtils.isBlank(dbcPaths)) {
            return new DbcNullDecoder();
        }
        
        final Set<Long> messageids = new HashSet<>(); 
        final MessageDecoder decoder = consolidateSchema ? new DbcConsolidateMessageDecoder(outputOffset) : new DbcChannelMessageDecoder(outputOffset);
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
                lines = FileOperationUtils.readFileToList(paths[i].trim(), false);
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
            
            //post-processing, remove invalid message, set output offset.
            final LinkedHashMap<Long, DbcMessage> messages = new LinkedHashMap<>();
            for (final Map.Entry<Long, DbcMessage> entry : channel.getMessages().entrySet()) {
                if (entry.getValue().size() > 0 && (!deduplicateMessage || !messageids.contains(entry.getKey()))) {
                    final Long messageid = entry.getKey();
                    final DbcMessage message = entry.getValue();
                    message.setOutputOffset(outputOffset);
                    messages.put(messageid, message);
                    messageids.add(messageid);
                }
            }
            
            if (messages.size() > 0) {
                channel.setMessages(messages);
                
                //add channel only after it has been processed
                if (consolidateSchema) {
                    ((DbcConsolidateMessageDecoder) decoder).addChannel(channel);
                } else {
                    ((DbcChannelMessageDecoder) decoder).addChannel(channel);
                }
            }
        }
        
        decoder.select(selectedAttributes);
        
        return decoder;
    }
    
    public static MessageDecoder buildDBCFromFileContents(
            final List<String> dbcFileContents,
            final boolean consolidateSchema,
            final boolean useQualifiedName,
            final boolean applyFormula,
            final boolean deduplicateMessage,
            final boolean outputOffset,
            final Set<String> selectedAttributes) throws EngineException {
        if (dbcFileContents == null || dbcFileContents.size() == 0) {
            return new DbcNullDecoder();
        }
        
        final Set<Long> messageids = new HashSet<>(); 
        final MessageDecoder decoder = consolidateSchema ? new DbcConsolidateMessageDecoder(outputOffset) : new DbcChannelMessageDecoder(outputOffset);
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
            
            //post-processing, remove invalid message, set output offset.
            final LinkedHashMap<Long, DbcMessage> messages = new LinkedHashMap<>();
            for (final Map.Entry<Long, DbcMessage> entry : channel.getMessages().entrySet()) {
                if (entry.getValue().size() > 0 && (!deduplicateMessage || !messageids.contains(entry.getKey()))) {
                    final Long messageid = entry.getKey();
                    final DbcMessage message = entry.getValue();
                    message.setOutputOffset(outputOffset);
                    messages.put(messageid, message);
                    messageids.add(messageid);
                }
            }
            
            if (messages.size() > 0) {
                channel.setMessages(messages);
                
                //add channel only after it has been processed
                if (consolidateSchema) {
                    ((DbcConsolidateMessageDecoder) decoder).addChannel(channel);
                } else {
                    ((DbcChannelMessageDecoder) decoder).addChannel(channel);
                }
            }
        }
        
        decoder.select(selectedAttributes);
        
        return decoder;
    }
    
    
    /*
    private static String[] CAN_FRAME_TRIGGERINGS_XPATH = new String[] {
            "AR-PACKAGES",
             "AR-PACKAGE",
             "ELEMENTS",
             "CAN-CLUSTER",
             "CAN-CLUSTER-VARIANTS",
             "CAN-CLUSTER-CONDITIONAL",
             "PHYSICAL-CHANNELS",
             "CAN-PHYSICAL-CHANNEL",
             "FRAME-TRIGGERINGS",
             "CAN-FRAME-TRIGGERING"};
    */
    public static MessageDecoder buildArxml(
            final String arxmlPaths, 
            final boolean consolidateSchema,
            final boolean useQualifiedName,
            final boolean applyFormula,
            final boolean deduplicateMessage,
            final boolean outputOffset,
            final Set<String> selectedAttributes) throws EngineException {
        if (XStringUtils.isBlank(arxmlPaths)) {
            return new DbcNullDecoder();
        }
        throw new EngineException("FORMAT_ARXML_NOT_YET_SUPPORTED");
        
        /*
        final DbcMessageDecoder decoder = new DbcMessageDecoder();
        final String[] paths = arxmlPaths.split(",");
        final int size = paths.length;
        DbcDelegator delegator;
        DbcChannel channel;
        Document doc;
        
  
        for (int i = 0; i < size; ++i) {
            channel = new DbcChannel().setChannelNumber((short) (i + 1));
            decoder.addChannel(channel);
            
            if (XStringUtils.isBlank(paths[i])) {
                continue;
            }
            delegator = null;
            doc = FileOperationUtils.readFileToDocument(paths[i], false, false);
            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            
            
            for (String xp : CAN_FRAME_TRIGGERINGS_XPATH) {
                final XPathExpression exp = xpath.compile(xp);
                final Object result = exp.evaluate(doc, XPathConstants.NODESET);
                final NodeList nodes = (NodeList) result;
                for (int j = 0, s = nodes.getLength(); j < s; ++j) {

                    delegator = DbcDelegatorFactory.build(line);
                    delegator.delegate(channel, line);
                }
            }
        }
        
        decoder.select(selectedAttributes);
        
        return decoder; */
    }
    
    public static boolean isDBC(final String dbcPaths) {
        if (XStringUtils.isBlank(dbcPaths)) {
            return false;
        }
        final String[] paths = dbcPaths.split(",");
        final int size = paths.length;
        for (int i = 0; i < size; ++i) {
            if (XStringUtils.isNotBlank(paths[i])) {
                if (!paths[i].trim().toLowerCase().endsWith(".dbc")) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    public static boolean isArxml(final String dbcPaths) {
        if (XStringUtils.isBlank(dbcPaths)) {
            return false;
        }
        final String[] paths = dbcPaths.split(",");
        final int size = paths.length;
        for (int i = 0; i < size; ++i) {
            if (XStringUtils.isNotBlank(paths[i])) {
                if (!paths[i].trim().toLowerCase().endsWith(".arxml")) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    public static Template toTemplate(final MessageDecoder decoder) {
        final Template template = new Template();
        if (decoder instanceof DbcChannelMessageDecoder) {
            template.put(TIME, new Desc(TIME, new PrimitiveDescType(Types.INSTANT), false));
            if (decoder.getOutputOffset()) {
                template.put(OFFSET, new Desc(OFFSET, new PrimitiveDescType(Types.LONG), false));
            }
            final DbcChannelMessageDecoder inspectorDBC = (DbcChannelMessageDecoder) decoder;
            for (final DbcChannel channel : inspectorDBC.getChannels().values()) {
                for (final DbcMessage message : channel.getMessages().values()) {
                    for (final DbcAttribute attribute : message.getAttributes()) {
                        addAttributeToTemplate(template, attribute);
                    }
                }
            } 
        } else if (decoder instanceof DbcConsolidateMessageDecoder) {
            template.put(TIME, new Desc(TIME, new PrimitiveDescType(Types.INSTANT), false));
            if (decoder.getOutputOffset()) {
                template.put(OFFSET, new Desc(OFFSET, new PrimitiveDescType(Types.LONG), false));
            }
            final DbcConsolidateMessageDecoder inspectorDBC = (DbcConsolidateMessageDecoder) decoder;
            for (final DbcMessage message : inspectorDBC.getMessages().values()) {
                for (final DbcAttribute attribute : message.getAttributes()) {
                    addAttributeToTemplate(template, attribute);
                }
            }
        }
        
        return template;
    }
    
    private static void addAttributeToTemplate(final Template template, final DbcAttribute attribute) {
        if (attribute instanceof DbcAttributeDecimal) {
            template.put(
                    attribute.getName(), 
                    new Desc(attribute.getName(), new PrimitiveDescType(Types.DECIMAL).setScale(attribute.isWhole() ? 0 : -1), false)
                    );
        } else if (attribute instanceof DbcAttributeLong) {
            template.put(
                    attribute.getName(), 
                    new Desc(attribute.getName(), new PrimitiveDescType(Types.LONG), false)
                    );
        } else if (attribute instanceof DbcAttributeInteger) {
            template.put(
                    attribute.getName(), 
                    new Desc(attribute.getName(), new PrimitiveDescType(Types.INT), false)
                    );
        } else if (attribute instanceof DbcAttributeDouble) {
            template.put(
                    attribute.getName(), 
                    new Desc(attribute.getName(), new PrimitiveDescType(Types.DOUBLE).setScale(attribute.isWhole() ? 0 : -1), false)
                    );
        } else {
            template.put(
                    attribute.getName(), 
                    new Desc(attribute.getName(), new PrimitiveDescType(Types.DECIMAL).setScale(attribute.isWhole() ? 0 : -1), false)
                    );
        }
    }
    
    public static Record toTemplateRecord(final MessageDecoder decoder) {
        final Record template = new Record();
        if (decoder instanceof DbcChannelMessageDecoder) {
            template.add(TIME, InstantData.NULL);
            if (decoder.getOutputOffset()) {
                template.add(OFFSET, LongData.NULL);
            }
            final DbcChannelMessageDecoder inspectorDBC = (DbcChannelMessageDecoder) decoder;
            for (final DbcChannel channel : inspectorDBC.getChannels().values()) {
                for (final DbcMessage message : channel.getMessages().values()) {
                    for (final DbcAttribute attribute : message.getAttributes()) {
                        addAttributeToTemplateRecord(template, attribute);
                    }
                }
            }
        } else if (decoder instanceof DbcConsolidateMessageDecoder) {
            template.add(TIME, InstantData.NULL);
            if (decoder.getOutputOffset()) {
                template.add(OFFSET, LongData.NULL);
            }
            final DbcConsolidateMessageDecoder inspectorDBC = (DbcConsolidateMessageDecoder) decoder;
            for (final DbcMessage message : inspectorDBC.getMessages().values()) {
                for (final DbcAttribute attribute : message.getAttributes()) {
                    addAttributeToTemplateRecord(template, attribute);
                }
            }
        }
        
        return template;
    }
    
    private static void addAttributeToTemplateRecord(final Record template, final DbcAttribute attribute) {
        if (attribute instanceof DbcAttributeLong) {
            template.add(attribute.getName(), LongData.NULL);
        } else if (attribute instanceof DbcAttributeInteger) {
            template.add(attribute.getName(), IntData.NULL);
        } else if (attribute instanceof DbcAttributeDouble) {
            template.add(attribute.getName(), DoubleData.NULL);
        } else {
            template.add(attribute.getName(), DecimalData.NULL);
        }
    }
    
    public static String toSchema(final MessageDecoder decoder) {
        final StringBuilder sb = new StringBuilder(4096);
        if (decoder instanceof DbcChannelMessageDecoder) {
            sb.append("TIME instant");
            if (decoder.getOutputOffset()) {
                sb.append("OFFSET long");
            }
            final DbcChannelMessageDecoder inspectorDBC = (DbcChannelMessageDecoder) decoder;
            for (final DbcChannel channel : inspectorDBC.getChannels().values()) {
                for (final DbcMessage message : channel.getMessages().values()) {
                    for (final DbcAttribute attribute : message.getAttributes()) {
                        addAttributeToSchemaBuilder(sb, attribute);
                    }
                }
            }
        } else if (decoder instanceof DbcConsolidateMessageDecoder) {
            sb.append("TIME instant");
            if (decoder.getOutputOffset()) {
                sb.append("OFFSET long");
            }
            final DbcConsolidateMessageDecoder inspectorDBC = (DbcConsolidateMessageDecoder) decoder;
            for (final DbcMessage message : inspectorDBC.getMessages().values()) {
                for (final DbcAttribute attribute : message.getAttributes()) {
                    addAttributeToSchemaBuilder(sb, attribute);
                }
            }
        }
        return sb.toString();
    }
    
    private static void addAttributeToSchemaBuilder(final StringBuilder sb, final DbcAttribute attribute) {
        if (attribute instanceof DbcAttributeDecimal) {
            sb.append(",").append(attribute.getName()).append(attribute.isWhole() ? " decimal(0)" : " decimal");
        } else if (attribute instanceof DbcAttributeLong) {
            sb.append(",").append(attribute.getName()).append(" long");
        } else if (attribute instanceof DbcAttributeInteger) {
            sb.append(",").append(attribute.getName()).append(" int");
        } else if (attribute instanceof DbcAttributeDouble) {
            sb.append(",").append(attribute.getName()).append(attribute.isWhole() ? " double(0)" : " double");
        } else {
            sb.append(",").append(attribute.getName()).append(attribute.isWhole() ? " decimal(0)" : " decimal");
        }
    }
}
