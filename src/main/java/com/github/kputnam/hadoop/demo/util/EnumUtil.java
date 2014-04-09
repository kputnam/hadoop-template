package com.github.kputnam.hadoop.demo.util;

/**
 * Created by kputnam on 4/9/14.
 */
public class EnumUtil {

    /**
     * Deterministic hashCode for Enum values, suitable as a replacement
     * for Enum#hashCode which is non-deterministic across JVM processes!
     */
    public static int hashCode(Enum x) {
        return x == null ? 0 : x.ordinal() ^ x.getClass().getName().hashCode();
    }
}
