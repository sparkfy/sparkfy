package com.github.sparkfy.util;

import com.google.common.collect.ImmutableMap;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huangyu on 15/10/11.
 */
public class JavaUtils {

    private static final ImmutableMap<String, TimeUnit> timeSuffixes =
            ImmutableMap.<String, TimeUnit>builder()
                    .put("us", TimeUnit.MICROSECONDS)
                    .put("ms", TimeUnit.MILLISECONDS)
                    .put("s", TimeUnit.SECONDS)
                    .put("m", TimeUnit.MINUTES)
                    .put("min", TimeUnit.MINUTES)
                    .put("h", TimeUnit.HOURS)
                    .put("d", TimeUnit.DAYS)
                    .build();

    private static final ImmutableMap<String, ByteUnit> byteSuffixes =
            ImmutableMap.<String, ByteUnit>builder()
                    .put("b", ByteUnit.BYTE)
                    .put("k", ByteUnit.KiB)
                    .put("kb", ByteUnit.KiB)
                    .put("m", ByteUnit.MiB)
                    .put("mb", ByteUnit.MiB)
                    .put("g", ByteUnit.GiB)
                    .put("gb", ByteUnit.GiB)
                    .put("t", ByteUnit.TiB)
                    .put("tb", ByteUnit.TiB)
                    .put("p", ByteUnit.PiB)
                    .put("pb", ByteUnit.PiB)
                    .build();

    /**
     * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count for
     * internal use. If no suffix is provided a direct conversion is attempted.
     */
    private static long parseTimeString(String str, TimeUnit unit) {
        String lower = str.toLowerCase().trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !timeSuffixes.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }

            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    /**
     * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in seconds.
     */
    public static long timeStringAsSec(String str) {
        return parseTimeString(str, TimeUnit.SECONDS);
    }


    /**
     * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in ms.
     */
    public static long timeStringAsMs(String str) {
        return parseTimeString(str, TimeUnit.MILLISECONDS);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to a ByteUnit for
     * internal use. If no suffix is provided a direct conversion of the provided default is
     * attempted.
     */
    private static long parseByteString(String str, ByteUnit unit) {
        String lower = str.toLowerCase().trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);

            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);

                // Check for invalid suffixes
                if (suffix != null && !byteSuffixes.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
                }

                // If suffix is valid use that, otherwise none was provided and use the default passed
                return unit.convertFrom(val, suffix != null ? byteSuffixes.get(suffix) : unit);
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException("Fractional values are not supported. Input was: "
                        + fractionMatcher.group(1));
            } else {
                throw new NumberFormatException("Failed to parse byte string: " + str);
            }

        } catch (NumberFormatException e) {
            String timeError = "Size must be specified as bytes (b), " +
                    "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
                    "E.g. 50b, 100k, or 250m.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in bytes.
     */
    public static long byteStringAsBytes(String str) {
        return parseByteString(str, ByteUnit.BYTE);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in kibibytes.
     */
    public static long byteStringAsKb(String str) {
        return parseByteString(str, ByteUnit.KiB);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in mebibytes.
     */
    public static long byteStringAsMb(String str) {
        return parseByteString(str, ByteUnit.MiB);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in gibibytes.
     */
    public static long byteStringAsGb(String str) {
        return parseByteString(str, ByteUnit.GiB);
    }
}
