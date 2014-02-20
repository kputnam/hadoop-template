package com.github.kputnam.hadoop.demo.words;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by kputnam on 2/19/14.
 */
public class NgramsTest {

    private MapReduceDriver<LongWritable, Text,
                            Text, IntWritable,
                            Text, IntWritable> driver;

    private LongWritable pos = new LongWritable(0);
    private IntWritable one  = new IntWritable(1);
    private IntWritable two  = new IntWritable(2);

    @Before
    public void before() {
        Ngrams.charMapper.setN(3);
        Ngrams.wordMapper.setN(3);

        driver = MapReduceDriver.newMapReduceDriver(
                new Ngrams.charMapper(),
                new Ngrams.reducer());
    }

    @Test
    public void testUniqChars() throws Exception {
        driver
            .withMapper(new Ngrams.charMapper())
            .withInput(pos, new Text("onetwo thrfou fivsix"))
            .withOutput(new Text("etw"), one)
            .withOutput(new Text("fiv"), one)
            .withOutput(new Text("fou"), one)
            .withOutput(new Text("hrf"), one)
            .withOutput(new Text("ivs"), one)
            .withOutput(new Text("net"), one)
            .withOutput(new Text("one"), one)
            .withOutput(new Text("rfo"), one)
            .withOutput(new Text("six"), one)
            .withOutput(new Text("thr"), one)
            .withOutput(new Text("two"), one)
            .withOutput(new Text("vsi"), one)
            .runTest();
    }

    @Test
    public void testDupChars() throws Exception {
        driver
            .withMapper(new Ngrams.charMapper())
            .withInput(pos, new Text("and then the hens"))
            .withOutput(new Text("and"), one)
            .withOutput(new Text("ens"), one)
            .withOutput(new Text("hen"), two)
            .withOutput(new Text("the"), two)
            .runTest();
    }

    @Test
    public void testUniqWords() throws Exception {
        driver
            .withMapper(new Ngrams.wordMapper())
            .withInput(pos, new Text("one two thr fou fiv six"))
            .withOutput(new Text("fou fiv six"), one)
            .withOutput(new Text("one two thr"), one)
            .withOutput(new Text("thr fou fiv"), one)
            .withOutput(new Text("two thr fou"), one)
            .runTest();
    }

    @Test
    public void testDupWords() throws Exception {
        driver
            .withMapper(new Ngrams.wordMapper())
            .withInput(pos, new Text("one two thr two thr two"))
            .withOutput(new Text("one two thr"), one)
            .withOutput(new Text("thr two thr"), one)
            .withOutput(new Text("two thr two"), two)
            .runTest();
    }
}
