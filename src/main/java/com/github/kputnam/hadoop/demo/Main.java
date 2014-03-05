package com.github.kputnam.hadoop.demo;

import com.github.kputnam.hadoop.demo.algebra.DotProduct;
import com.github.kputnam.hadoop.demo.metrics.WordCountInstrumented;
import com.github.kputnam.hadoop.demo.words.Histogram;
import com.github.kputnam.hadoop.demo.words.Ngrams;
import com.github.kputnam.hadoop.demo.words.TopWords;
import com.github.kputnam.hadoop.demo.words.WordCount;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kputnam on 2/10/14.
 */
public class Main {
    final static Map<String, Class<? extends Tool>> tools =
            new HashMap<String, Class<? extends Tool>>();

    static {
        tools.put("ngrams", Ngrams.class);
        tools.put("topwords", TopWords.class);
        tools.put("wordcount", WordCount.class);
        tools.put("wordcount-instrumented", WordCountInstrumented.class);
        tools.put("histogram", Histogram.class);
        tools.put("dotproduct", DotProduct.class);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || !tools.containsKey(args[0]))
            usage();

        Tool tool = tools.get(args[0]).newInstance();
        String[] rest = Arrays.copyOfRange(args, 1, args.length);

        System.exit(ToolRunner.run(tool, rest));
    }

    private static void usage() {
        System.err.println("usage: hadoop -jar <...> <wordcount|ngrams|histogram|...> <args...>");
        System.exit(-1);
    }
}
