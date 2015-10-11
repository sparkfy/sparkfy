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
}
