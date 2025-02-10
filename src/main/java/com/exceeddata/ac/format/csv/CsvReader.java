package com.exceeddata.ac.format.csv;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.TimeZone;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.data.template.Desc;
import com.exceeddata.ac.common.data.template.Template;
import com.exceeddata.ac.common.data.template.TemplateBuilder;
import com.exceeddata.ac.common.data.type.Types;
import com.exceeddata.ac.common.data.typedata.BooleanData;
import com.exceeddata.ac.common.data.typedata.CalendarTimeData;
import com.exceeddata.ac.common.data.typedata.CalendarTimestampData;
import com.exceeddata.ac.common.data.typedata.ComplexData;
import com.exceeddata.ac.common.data.typedata.DataConv;
import com.exceeddata.ac.common.data.typedata.DateData;
import com.exceeddata.ac.common.data.typedata.DecimalData;
import com.exceeddata.ac.common.data.typedata.DenseVectorData;
import com.exceeddata.ac.common.data.typedata.DoubleData;
import com.exceeddata.ac.common.data.typedata.FloatData;
import com.exceeddata.ac.common.data.typedata.InstantData;
import com.exceeddata.ac.common.data.typedata.IntData;
import com.exceeddata.ac.common.data.typedata.LongData;
import com.exceeddata.ac.common.data.typedata.NullData;
import com.exceeddata.ac.common.data.typedata.NumericData;
import com.exceeddata.ac.common.data.typedata.SparseVectorData;
import com.exceeddata.ac.common.data.typedata.StringData;
import com.exceeddata.ac.common.data.typedata.TimeData;
import com.exceeddata.ac.common.data.typedata.TimestampData;
import com.exceeddata.ac.common.data.typedata.TypeData;
import com.exceeddata.ac.common.exception.EngineException;
import com.exceeddata.ac.common.extern.ExternRecordReader;
import com.exceeddata.ac.common.util.SparseArray;
import com.exceeddata.ac.common.util.XBooleanUtils;
import com.exceeddata.ac.common.util.XNumberUtils;
import com.exceeddata.ac.common.util.XStringUtils;
import com.exceeddata.ac.common.util.XTypeDataUtils;

public class CsvReader implements ExternRecordReader, Serializable {
    private static final long serialVersionUID = 1L;

    private Reader reader = null;
    private InputStream istream = null;
    private String charset = "UTF-8";
    
    private CsvLineParser parser = new CsvLineParser(',', '"', '\\', ' ', true);  
    private SparseArray<String> strings = new SparseArray<String>();
    private StringBuilder row = new StringBuilder(4096);
    private StringBuilder sbuffer = new StringBuilder(32);
    private int previousChar = -1;
    private boolean readHeader = true;
    
    private Template template = null;
    private Record templateable = null;
    private int elementCount = 0;
    private Record current = null;
    

    //user custom formats and null string
    protected String nullString = "null";
    protected TimeZone timeZone = null;
    protected NumberFormat customDecimalFormat = null;
    protected DateTimeFormatter customDateFormat = null;
    protected DateTimeFormatter customTimeFormat = null;
    protected DateTimeFormatter customTimestampFormat = null;
    protected DateTimeFormatter customTimeWithTimeZoneFormat = null;
    protected DateTimeFormatter customTimestampWithTimeZoneFormat = null;
    protected DateTimeFormatter customInstantFormat = null;
    protected TimeZone customDefaultTimeZone = null;
    protected boolean hasCustomNull = false;
    protected boolean hasCustomDecimal = false;
    protected boolean hasCustomDate = false;
    protected boolean hasCustomTime = false;
    protected boolean hasCustomTimestamp = false;
    protected boolean hasCustomTimeWithZone = false;
    protected boolean hasCustomTimestampWithZone = false;
    protected boolean hasCustomInstant = false;
    protected boolean hasCustomTimeZone = false;
    
    public CsvReader() {
    }
    
    public CsvReader(final String path) throws IOException {
    	open(path);
    }
    
    public CsvReader(final InputStream stream) throws IOException {
    	open(stream);
    }
    
    public CsvReader open(final String path) throws IOException {
        istream = new BufferedInputStream(Files.newInputStream(Paths.get(path), StandardOpenOption.READ));
        reader = new InputStreamReader(istream, charset);
        return this;
    }
    
    public CsvReader open(final InputStream stream) throws IOException {
        this.istream = stream;
        this.reader = new InputStreamReader(istream, charset);
        return this;
    }
    
    public CsvReader setNullString(final String nullString) {
        if (XStringUtils.isNotBlank(nullString)) {
            this.nullString = nullString;
            this.hasCustomNull = true;
        } else {
            this.nullString = "null";
            this.hasCustomNull = false;
        }
        return this;
    }
    
    public CsvReader setDecimalFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customDecimalFormat = new DecimalFormat(fmt);
            this.hasCustomDecimal = true;
        } else {
            this.customDecimalFormat = null;
            this.hasCustomDecimal = false;
        }
        return this;
    }
    
    public CsvReader setDateFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customDateFormat = DateTimeFormatter.ofPattern(fmt);
            this.hasCustomDate = true;
        } else {
            this.customDecimalFormat = null;
            this.hasCustomDate = false;
        }
        return this;
    }
    
    public CsvReader setTimeFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customTimeFormat = DateTimeFormatter.ofPattern(fmt);
            this.hasCustomTime = true;
        } else {
            this.customTimeFormat = null;
            this.hasCustomTime = false;
        }
        return this;
    }
    
    public CsvReader setTimestampFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customTimestampFormat = DateTimeFormatter.ofPattern(fmt);
            this.hasCustomTimestamp = true;
        } else {
            this.customTimestampFormat = null;
            this.hasCustomTimestamp = false;
        }
        return this;
    }
    
    public CsvReader setTimeWithZoneFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customTimeWithTimeZoneFormat = DateTimeFormatter.ofPattern(fmt);
            this.hasCustomTimeWithZone = true;
        } else {
            this.customTimeWithTimeZoneFormat = null;
            this.hasCustomTimeWithZone = false;
        }
        return this;
    }
    
    public CsvReader setTimestampWithZoneFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customTimestampWithTimeZoneFormat = DateTimeFormatter.ofPattern(fmt);
            this.hasCustomTimestampWithZone = true;
        } else {
            this.customTimestampWithTimeZoneFormat = null;
            this.hasCustomTimestampWithZone = false;
        }
        return this;
    }
    
    public CsvReader setInstantFormat(final String fmt) {
        if (XStringUtils.isNotBlank(fmt)) {
            this.customInstantFormat = DateTimeFormatter.ofPattern(fmt);
            this.hasCustomInstant = true;
        } else {
            this.customInstantFormat = null;
            this.hasCustomInstant = false;
        }
        return this;
    }
    
    public CsvReader setTimeZone(final String zone) {
        if (XStringUtils.isNotBlank(zone)) {
            this.customDefaultTimeZone = TimeZone.getTimeZone(zone);
            this.hasCustomTimeZone = true;
        } else {
            this.customDefaultTimeZone = null;
            this.hasCustomTimeZone = false;
        }
        return this;
    }
    
    @Override
    public boolean next() throws IOException {
        String value = null;
        
        while (nextRow()) {
            value = row.toString().trim();
            if (value.length() == 0) {
                continue;
            }
            
            if (readHeader) {
                final SparseArray<String> values = parser.splitUnsafe(value, strings, sbuffer);
                if (values == null || values.getSize() == 0) {
                    continue;
                }
                
                if (!metarize(values)) {
                    continue;
                }
                
                readHeader = false;
                continue;
            }

            final SparseArray<String> values = parser.splitUnsafe(value, strings, sbuffer);
            current = buildTemplate(values);
            return true;
        }
        
        return false;
    }

    @Override
    public Record get() {
        return current;
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
                    	if (i != '"') {
                    		quoted = false;
                    	}
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

    private boolean metarize(final SparseArray<String> strings) throws EngineException {
        String schema = CsvUtils.getHeaderSchema(strings, true);
        if (XStringUtils.isBlank(schema)) {
            final List<String> values = strings.datas();
            int vsize = strings.getSize();
            if (vsize != values.size()) {
                //validity check, sometimes header may contain extra comma at end
                while (vsize > values.size() && XStringUtils.isBlank(strings.get(vsize - 1))) {
                    --vsize;
                }
            }
            if (vsize == 0) {
                return false;
            }
            
            final StringBuilder sb = new StringBuilder(4096);
            for (int i = 0; i < vsize; ++i) {
                sb.append("field"+i + ",");
            }
            sb.setLength(sb.length() - 1);
            schema = sb.toString();
        }
        template = TemplateBuilder.build(schema);
        templateable = new Record();
        final int fieldCount = template.size();
        for (int i = 0; i < fieldCount; ++i) {
            templateable.add(template.descAt(i).getName(), NullData.INSTANCE);
        }
        elementCount = templateable.size();
        
        return true;
    }
    
    protected Record buildTemplate (final SparseArray<String> strings) throws EngineException {
        final List<Integer> indices = strings.indices();
        final List<String> datas = strings.datas();
        Desc desc;
        int index;
        
        final Record value = templateable.dataCopy();
        final TypeData[] vdatas = value.unsafeDatas();
        for (int i = 0, length = indices.size(); i < length; ++i) {
            index = indices.get(i);
            if (index >= elementCount) {
                break; //no need to go further, not defined in schema and presumably not needed
            }
            desc = template.descAt(index);
            vdatas[index] = desc.transform(convertTypeData(datas.get(i), desc)) ;
        }
        return value;
    }
    
    protected TypeData convertTypeData(
            final String s, 
            final Desc desc) throws EngineException {
        switch (desc.getDescType().getType()) {
        case Types.INT:
            try {
                return XStringUtils.isBlank(s) ? IntData.NULL : IntData.valueOf(Double.valueOf(s).intValue());
            } catch (NumberFormatException e) {
                if (desc.getNullable() || nullString.equalsIgnoreCase(s)) {
                    return IntData.NULL;
                } else {
                    throw new EngineException("DATA_STRING_TO_INT_CONVERSION_INVALID: " + s);
                }
            }
        case Types.LONG:
            try {
                if (XStringUtils.isBlank(s)) {
                    return LongData.NULL;
                } else if (XNumberUtils.isDigits(s)) {
                    return LongData.valueOf(Long.valueOf(s));
                } else {
                    return LongData.valueOf(Double.valueOf(s).longValue());
                }
            } catch (NumberFormatException e) {
                if (desc.getNullable() || nullString.equalsIgnoreCase(s)) {
                    return LongData.NULL;
                } else {
                    throw new EngineException("DATA_STRING_TO_LONG_CONVERSION_INVALID: " + s);
                }
            }
        case Types.FLOAT:
            if (XStringUtils.isBlank(s)) {
                return FloatData.NULL;
            } else {
                if (hasCustomDecimal) {
                    try {
                        return FloatData.valueOf(customDecimalFormat.parse(s)
                                .floatValue());
                    } catch (ParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return FloatData.NULL;
                        }
                    }
                }
                try {
                    return FloatData.valueOf(Float.valueOf(s));
                } catch (NumberFormatException e) {
                    if (!hasCustomDecimal
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return FloatData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_FLOAT_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.DOUBLE:
            if (XStringUtils.isBlank(s)) {
                return DoubleData.NULL;
            } else {
                if (hasCustomDecimal) {
                    try {
                        return DoubleData.valueOf(customDecimalFormat.parse(s)
                                .doubleValue());
                    } catch (ParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return DoubleData.NULL;
                        }
                    }
                }
                try {
                    return DoubleData.valueOf(Double.valueOf(s));
                } catch (NumberFormatException e) {
                    if (!hasCustomDecimal
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return DoubleData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_DOUBLE_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.NUMERIC:
            if (XStringUtils.isBlank(s)) {
                return NumericData.NULL;
            } else {
                if (hasCustomDecimal) {
                    try {
                        return NumericData.valueOf(customDecimalFormat.parse(s)
                                .doubleValue());
                    } catch (ParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return NumericData.NULL;
                        }
                    }
                }
                try {
                    return NumericData.valueOf(Double.valueOf(s));
                } catch (NumberFormatException e) {
                    if (!hasCustomDecimal
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return NumericData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_NUMERIC_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.DECIMAL:
            if (XStringUtils.isBlank(s)) {
                return DecimalData.NULL;
            } else {
                if (hasCustomDecimal) {
                    try {
                        return DecimalData.valueOf(new BigDecimal(
                                customDecimalFormat.parse(s).doubleValue()));
                    } catch (ParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return DecimalData.NULL;
                        }
                    }
                }
                try {
                    return DecimalData.valueOf(new BigDecimal(s));
                } catch (NumberFormatException e) {
                    if (!hasCustomDecimal
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return DecimalData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_DECIMAL_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.COMPLEX:
            return ComplexData.valueOf(s);
        case Types.BOOLEAN:
            return BooleanData.valueOf(XBooleanUtils.getBoolean(s));
        case Types.DATE:
            if (XStringUtils.isBlank(s)) {
                return DateData.NULL;
            } else {
                if (hasCustomDate) {
                    try {
                        return DateData.valueOf(s, customDateFormat);
                    } catch (DateTimeParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return DateData.NULL;
                        }
                    }
                }
                try {
                    return DateData.valueOf(s);
                } catch (DateTimeParseException e) {
                    if (!hasCustomDate
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return DateData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_DATE_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.TIME:
            if (XStringUtils.isBlank(s)) {
                return TimeData.NULL;
            } else {
                if (hasCustomTime) {
                    try {
                        return new TimeData(s, customTimeFormat);
                    } catch (DateTimeParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return TimeData.NULL;
                        } else {
                            throw new EngineException("DATA_STRING_TO_TIME_CONVERSION_INVALID: " + s);
                        }
                    }
                }
                try {
                    return new TimeData(s);
                } catch (DateTimeParseException e) {
                    if (!hasCustomTime && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return TimeData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_TIME_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.TIMESTAMP:
            if (XStringUtils.isBlank(s)) {
                return TimestampData.NULL;
            } else {
                if (hasCustomTimestamp) {
                    try {
                        return TimestampData.valueOf(s, customTimestampFormat);
                    } catch (DateTimeParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return TimestampData.NULL;
                        } else {
                            throw new EngineException("DATA_STRING_TO_TIMESTAMP_CONVERSION_INVALID: " + s);
                        }
                    }
                }
                
                try {
                    return TimestampData.valueOf(s);
                } catch (DateTimeParseException e) {
                    if (!hasCustomTimestamp
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return TimestampData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_TIMESTAMP_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.CALENDAR_TIME:
            if (XStringUtils.isBlank(s)) {
                return CalendarTimeData.NULL;
            } else {
                if (hasCustomTimeWithZone) {
                    try {
                        return new CalendarTimeData(s, customTimeWithTimeZoneFormat);
                    } catch (DateTimeParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return CalendarTimeData.NULL;
                        }
                    }
                }
                
                try {
                    return new CalendarTimeData(s);
                } catch (DateTimeParseException e) {
                    if (!hasCustomTimeWithZone
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return CalendarTimeData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_TIMEWITHTIMEZONE_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.CALENDAR_TIMESTAMP:
            if (XStringUtils.isBlank(s)) {
                return CalendarTimestampData.NULL;
            } else {
                if (hasCustomTimestampWithZone) {
                    try {
                        return new CalendarTimestampData(s, customTimestampWithTimeZoneFormat);
                    } catch (DateTimeParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return CalendarTimestampData.NULL;
                        }
                    }
                }
                
                try {
                    return new CalendarTimestampData(s);
                } catch (DateTimeParseException e) {
                    if (!hasCustomTimestampWithZone
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return CalendarTimestampData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_TIMESTAMPWITHTIMEZONE_CONVERSION_INVALID: " + s);
                    }
                }
            }

        case Types.INSTANT:
            if (XStringUtils.isBlank(s)) {
                return InstantData.NULL;
            } else {
                if (hasCustomInstant) {
                    try {
                        return new InstantData(s, customInstantFormat);
                    } catch (DateTimeParseException e) {
                        if (desc.getNullable() && nullString.equalsIgnoreCase(s)) {
                            return InstantData.NULL;
                        } else {
                            throw new EngineException("DATA_STRING_TO_INSTANT_CONVERSION_INVALID: " + s);
                        }
                    }
                }
                try {
                    return new InstantData(s);
                } catch (DateTimeParseException e) {
                    if (!hasCustomInstant
                            && (desc.getNullable() || nullString.equalsIgnoreCase(s))) {
                        return InstantData.NULL;
                    } else {
                        throw new EngineException("DATA_STRING_TO_INSTANT_CONVERSION_INVALID: " + s);
                    }
                }
            }
        case Types.LIST:
            return XTypeDataUtils.jsonToListData(s);
        case Types.SET:
            return XTypeDataUtils.jsonToSetData(s);
        case Types.MAP:
            return XTypeDataUtils.jsonToMapData(s);
        case Types.DENSEVECTOR:
            try {
                return DataConv.toDenseVectorData(new StringData(s));
            } catch(EngineException e) {
                if (desc.getNullable() || nullString.equalsIgnoreCase(s)) {
                    return DenseVectorData.NULL;
                } else {
                    throw new EngineException("DATA_STRING_TO_DENSEVECTOR_CONVERSION_INVALID: " + s);
                }
            }
        case Types.SPARSEVECTOR:
            try {
                return DataConv.toSparseVectorData(new StringData(s));
            } catch(EngineException e) {
                if (desc.getNullable() || nullString.equalsIgnoreCase(s)) {
                    return SparseVectorData.NULL;
                } else {
                    throw new EngineException("DATA_STRING_TO_SPARSEVECTOR_CONVERSION_INVALID: " + s);
                }
            }
        default:
            if (hasCustomNull && nullString.equalsIgnoreCase(s)) {
                return StringData.NULL;
            } else {
                return StringData.nonNullValueOf(s);
            }
        }
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
