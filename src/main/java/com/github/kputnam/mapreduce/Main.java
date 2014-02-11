package com.github.kputnam.mapreduce;

import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Created by kputnam on 2/10/14.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 1)
            usage();

        String[] rest = Arrays.copyOfRange(args, 1, args.length);

        if ("ngrams".equals(args[0]))
            System.exit(ToolRunner.run(new Ngrams(), rest));

        if ("wordcount".equals(args[0]))
            System.exit(ToolRunner.run(new WordCount(), rest));

        if ("histogram".equals(args[0]))
            System.exit(ToolRunner.run(new Histogram(), rest));

        usage();
    }

    private static void usage() {
        System.err.println("usage: hadoop -jar <...> <wordcount|ngrams|...> <args...>");
        System.exit(-1);
    }
}
