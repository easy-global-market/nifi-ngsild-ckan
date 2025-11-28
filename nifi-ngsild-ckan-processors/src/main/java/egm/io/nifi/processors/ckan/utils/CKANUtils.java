package egm.io.nifi.processors.ckan.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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

    public static int generateHash(String input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        // Use the first 4 bytes to create a 32-bit integer
        return ((hashBytes[0] & 0xff) << 24) |
            ((hashBytes[1] & 0xff) << 16) |
            ((hashBytes[2] & 0xff) << 8)  |
            (hashBytes[3] & 0xff);
    }
}
