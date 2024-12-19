package com.exceeddata.ac.format.asc;

import static java.time.temporal.ChronoField.AMPM_OF_DAY;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.HOUR_OF_AMPM;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.exceeddata.ac.common.exception.EngineException;
import com.exceeddata.ac.common.util.XTemporalUtils;

public final class AscUtils {
    private static final Map<Long, String> moy = new HashMap<>();
    private static final Map<Long, String> ampm = new HashMap<>();
    static {
        moy.put(1L, "Jan");
        moy.put(2L, "Feb");
        moy.put(3L, "Mar");
        moy.put(4L, "Apr");
        moy.put(5L, "May");
        moy.put(6L, "Jun");
        moy.put(7L, "Jul");
        moy.put(8L, "Aug");
        moy.put(9L, "Sep");
        moy.put(10L, "Oct");
        moy.put(11L, "Nov");
        moy.put(12L, "Dec");
        ampm.put(0l, "am");
        ampm.put(1l, "pm");
    }
    
    private AscUtils() {}
    
    public static Long parseASCDateTime(final String line) throws IOException {
        String tm = line.trim();
        if (tm.toLowerCase().startsWith("begin")) {
            final int index = line.toLowerCase().indexOf("triggerblock");
            tm = line.substring(index + "triggerblock".length()).trim().replaceAll("\\s+", " ");
        } else {
            tm = line.substring(4).trim().replaceAll("\\s+", " ");
        }
        
        int index = tm.indexOf(' ');
        if (index > 0) {
            final String sub = tm.substring(0, index).trim().toLowerCase();
            switch(sub) {
                case "mon":
                case "tue":
                case "wed":
                case "thu":
                case "fri":
                case "sat":
                case "sun":
                    tm = tm.substring(index + 1).trim(); //remove useless day of week, causing problems when device generates it wrong
            }
        }
        
        Long timestamp;
        if ((timestamp = XTemporalUtils.parseTimestamp(tm, getASCDateTimeParser())) != null) {
            return validateTime(timestamp, tm);
        }
        if ((timestamp = XTemporalUtils.parseTimestamp(tm, getASCDateTimeParser2())) != null) {
            return timestamp;
        }
        if ((timestamp = XTemporalUtils.parseTimestamp(tm, getASCDateTimeParser3())) != null) {
            return validateTime(timestamp, tm);
        }
        
        if ((timestamp = XTemporalUtils.parseTimestamp(tm, getASCDateTimeParser4())) != null) {
            return timestamp;
        }
        
        if ((timestamp = XTemporalUtils.parseTimestamp(tm, getASCDateTimeParser5())) != null) {
            return timestamp;
        }
        
        if ((timestamp = XTemporalUtils.parseTimestamp(tm, getASCDateTimeParser6())) != null) {
            return timestamp;
        }
        
        throw new EngineException("FORMAT_ASC_DATE_FORMAT_NOT_SUPPORTED: " + tm);
    }
    
    public static String formatASCDateTime(final Instant time) {
        return getASCDateTimeFormatter().format(time.atZone(ZoneId.systemDefault()));
    }
    
    private static long validateTime(final long millis, final String tm) {
        final Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeInMillis(millis);
        if (cal.get(Calendar.HOUR_OF_DAY) == 12){
            if (tm.indexOf(" am ") > 0) {
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
        }
        return millis;
    }
    
    private static DateTimeFormatter getASCDateTimeParser() {
        return new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .parseLenient()
                    .appendText(MONTH_OF_YEAR, moy)
                    .appendLiteral(' ')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral(' ')
                    .appendValue(HOUR_OF_AMPM, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .optionalStart()
                    .appendLiteral('.')
                    .appendValue(MILLI_OF_SECOND, 1, 3, SignStyle.NOT_NEGATIVE)
                    .optionalEnd()
                    .appendLiteral(' ')
                    .appendText(AMPM_OF_DAY, ampm)
                    .appendLiteral(' ')
                    .appendValue(YEAR, 4)
                    .toFormatter();
    }
    
    private static DateTimeFormatter getASCDateTimeParser2() {
        return new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .parseLenient()
                    .appendText(MONTH_OF_YEAR, moy)
                    .appendLiteral(' ')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral(' ')
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .optionalStart()
                    .appendLiteral('.')
                    .appendValue(MILLI_OF_SECOND, 1, 3, SignStyle.NOT_NEGATIVE)
                    .optionalEnd()
                    .appendLiteral(' ')
                    .appendValue(YEAR, 4)
                    .toFormatter();
    }
    
    private static DateTimeFormatter getASCDateTimeParser3() {
        return new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .parseLenient()
                    .appendText(MONTH_OF_YEAR, moy)
                    .appendLiteral(' ')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral(' ')
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .optionalStart()
                    .appendLiteral('.')
                    .appendValue(MILLI_OF_SECOND, 1, 3, SignStyle.NOT_NEGATIVE)
                    .optionalEnd()
                    .optionalStart()
                    .appendLiteral(' ')
                    .appendText(AMPM_OF_DAY, ampm)
                    .optionalEnd()
                    .appendLiteral(' ')
                    .appendValue(YEAR, 4)
                    .toFormatter();
    }

    private static DateTimeFormatter getASCDateTimeParser4() {
        return new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .parseLenient()
                .appendValue(YEAR, 4)
                .appendLiteral('-')
                .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral('-')
                .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                .optionalStart()
                .appendLiteral(' ')
                .optionalEnd()
                .optionalStart()
                .appendLiteral('T')
                .optionalEnd()
                .optionalStart()
                .appendLiteral(' ')
                .optionalEnd()
                .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
                .toFormatter();
    }
    
    private static DateTimeFormatter getASCDateTimeParser5() {
        return new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .parseLenient()
                .appendValue(YEAR, 4)
                .appendLiteral("年")
                .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral("月")
                .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral("日")
                .appendLiteral(' ')
                .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
                .toFormatter();
    }
    
    private static DateTimeFormatter getASCDateTimeParser6() {
        return new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .parseLenient()
                .appendText(MONTH_OF_YEAR, moy)
                .appendLiteral(' ')
                .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(' ')
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
                .appendLiteral(' ')
                .appendZoneText( TextStyle.SHORT  )
                .appendLiteral(' ')
                .appendValue(YEAR, 4)
                .toFormatter();
    }
    
    private static DateTimeFormatter getASCDateTimeFormatter() {
        final Map<Long, String> dow = new HashMap<>();
        dow.put(1L, "Mon");
        dow.put(2L, "Tue");
        dow.put(3L, "Wed");
        dow.put(4L, "Thu");
        dow.put(5L, "Fri");
        dow.put(6L, "Sat");
        dow.put(7L, "Sun");
        final Map<Long, String> moy = new HashMap<>();
        moy.put(1L, "Jan");
        moy.put(2L, "Feb");
        moy.put(3L, "Mar");
        moy.put(4L, "Apr");
        moy.put(5L, "May");
        moy.put(6L, "Jun");
        moy.put(7L, "Jul");
        moy.put(8L, "Aug");
        moy.put(9L, "Sep");
        moy.put(10L, "Oct");
        moy.put(11L, "Nov");
        moy.put(12L, "Dec");
        final Map<Long, String> ampm = new HashMap<>();
        ampm.put(0l, "am");
        ampm.put(1l, "pm");
        return new DateTimeFormatterBuilder()
                    .appendText(DAY_OF_WEEK, dow)
                    .appendLiteral(' ')
                    .appendText(MONTH_OF_YEAR, moy)
                    .appendLiteral(' ')
                    .appendValue(DAY_OF_MONTH, 2)
                    .appendLiteral(' ')
                    .appendValue(HOUR_OF_AMPM, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .appendLiteral('.')
                    .appendValue(MILLI_OF_SECOND, 3)
                    .appendLiteral(' ')
                    .appendText(AMPM_OF_DAY, ampm)
                    .appendLiteral(' ')
                    .appendValue(YEAR, 4)
                    .toFormatter();
    }
    
    public static int padDLC(final int length) {
        switch(length) {
            case 0: return 0;
            case 1: return 1;
            case 2: return 2;
            case 3: return 3;
            case 4: return 4;
            case 5: return 5;
            case 6: return 6;
            case 7: return 7;
            case 8: return 8;
            case 9: 
            case 10: 
            case 11: 
            case 12: return 12; 
            case 13: 
            case 14:
            case 15: 
            case 16: return 16;
            case 17: 
            case 18:
            case 19: 
            case 20: return 20;
            case 21: 
            case 22:
            case 23: 
            case 24: return 24;
            case 32: return 32;
            case 48: return 48;
            case 64: return 64;
        }
        if (length < 48) {
            return length < 32 ? 32 : 48;
        }
        return 64;
    }
    
    public static String buildDLC(final int length) {
        switch(length) {
            case 0: return "0";
            case 1: return "1";
            case 2: return "2";
            case 3: return "3";
            case 4: return "4";
            case 5: return "5";
            case 6: return "6";
            case 7: return "7";
            case 8: return "8";
            case 9: 
            case 10: 
            case 11: 
            case 12: return "9"; 
            case 13: 
            case 14: 
            case 15: 
            case 16: return "a"; 
            case 17: 
            case 18: 
            case 19: 
            case 20: return "b"; 
            case 21: 
            case 22: 
            case 23: 
            case 24: return "c"; 
            case 32: return "d";
            case 48: return "e";
            case 64: return "f";
        }
        if (length < 48) {
            return length < 32 ? "d" : "e";
        }
        return "f";
    }
    
    public static String sampleExtInfo(final boolean tx) {
        if (tx) {
            return "  0 0 303040 0 0 0 0 0";
        } else {
            return "  0 0 303000 0 0 0 0 0";
        }
    }
}
