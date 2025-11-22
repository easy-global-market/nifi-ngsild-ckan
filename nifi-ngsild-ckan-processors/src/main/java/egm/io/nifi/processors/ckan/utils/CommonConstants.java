package egm.io.nifi.processors.ckan.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class CommonConstants {

    // Maximum values
    public static final int MAX_CONNS = 500;
    public static final int MAX_CONNS_PER_ROUTE = 100;

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private CommonConstants() {
    } // CommonConstants

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
    } // getHumanRedable

} // CommonConstants
