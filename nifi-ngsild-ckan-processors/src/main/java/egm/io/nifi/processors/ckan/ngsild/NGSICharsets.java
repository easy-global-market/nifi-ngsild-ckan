package egm.io.nifi.processors.ckan.ngsild;

import java.util.regex.Pattern;

public final class NGSICharsets {

    private static final Pattern ENCODEPATTERN = Pattern.compile("[^a-zA-Z0-9\\.\\-]");
    private static final Pattern ENCODEPATTERNSLASH = Pattern.compile("[^a-zA-Z0-9\\.\\-\\/]");

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private NGSICharsets() {
    } // NGSICharsets

    /**
     * Encodes a string for CKAN. Only lowercase alphanumerics, - and _ are allowed.
     * @param in
     * @return The encoded string
     */
    public static String encodeCKAN(String in) {
        String out = "";

        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            int code = c;

            if (code >= 97 && code <= 122) { // a-z --> a-z
                out += c;
            }  else if (code >= 48 && code <= 57) { // 0-9 --> 0-9
                out += c;
            } else if (c == '_' || c == '-' ) {
                out += c;
            } else { // --> any other special character
                out += '-';
            } // else
        } // for

        return out;
    } // encodeCKAN

    /**
     * Encodes a string replacing all the non alphanumeric characters by '_' (except by '-' and '.').
     * This should be only called when building a persistence element name, such as table names, file paths, etc.
     *
     * @return The encoded version of the input string.
     */
    public static String encode(String in, boolean deleteSlash, boolean encodeSlash) {
        if (deleteSlash) {
            return ENCODEPATTERN.matcher(in.substring(1)).replaceAll("_");
        } else if (encodeSlash) {
            return ENCODEPATTERN.matcher(in).replaceAll("_");
        } else {
            return ENCODEPATTERNSLASH.matcher(in).replaceAll("_");
        } // if else
    } // encode
} // NGSICharsets
