package egm.io.nifi.processors.ckan.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

public final class CKANUtils {

    private static final Pattern ENCODEPATTERN = Pattern.compile("[^a-zA-Z0-9\\_\\-]");

    /**
     * Gets the human redable version of timestamp expressed in miliseconds.
     */
    public static String getHumanReadable(long ts, boolean addUTC) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String humanRedable = sdf.format(new Date(ts));
        humanRedable += (addUTC ? "T" : " ");
        sdf = new SimpleDateFormat("HH:mm:ss.S");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        humanRedable += sdf.format(new Date(ts)) + (addUTC ? "Z" : "");
        return humanRedable;
    }

    public static String encodeCKAN(String in) {
        return ENCODEPATTERN.matcher(in).replaceAll("-");
    }
}
