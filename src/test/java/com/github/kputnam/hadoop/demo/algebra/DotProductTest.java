package com.github.kputnam.hadoop.demo.algebra;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kputnam on 2/19/14.
 */
public class DotProductTest {

    private MapReduceDriver<NullWritable, Pair,
                            NullWritable, DoubleWritable,
                            NullWritable,DoubleWritable> driver;

    private NullWritable nul = NullWritable.get();
    private DoubleWritable zed = new DoubleWritable(0);

    @Before
    public void before() {
        driver = MapReduceDriver.newMapReduceDriver(
            new DotProduct.mapper(),
            new DotProduct.reducer());
    }

    private void compareImpls(double[] as, double[] bs) throws Exception {
        int length = Math.min(as.length, bs.length);

        // First compute the simple, sequential way
        double expected = 0f;
        for (int k = 0; k < length; k ++)
            expected += as[k] * bs[k];

        // Now compute in parallel
        List<org.apache.hadoop.mrunit.types.Pair<NullWritable, Pair>> input =
                new ArrayList<org.apache.hadoop.mrunit.types.Pair<NullWritable, Pair>>();

        for (int k = 0; k < length; k++)
            input.add(new org.apache.hadoop.mrunit.types.Pair<NullWritable, Pair>(nul,
                    Pair.of(new DoubleWritable(as[k]), new DoubleWritable(bs[k]))));

        driver.withAll(input).withOutput(nul, new DoubleWritable(expected)).runTest();
    }

    @Test
    public void testLeftZero() throws Exception {
        compareImpls(
            new double[] { 0d, 0d, 0d },
            new double[] { 1d, 2d, 3d });
    }

    @Test
    public void testRightZero() throws Exception {
        compareImpls(
            new double[] { 1d, 2d, 3d },
            new double[] { 0d, 0d, 0d });
    }

    @Test
    public void testLeftOne() throws Exception {
        compareImpls(
            new double[] { 1d, 1d, 1d, 1d },
            new double[] { 2d, 3d, 4d, 5d });
    }

    @Test
    public void testRightOne() throws Exception {
        compareImpls(
            new double[] { 2d, 3d, 4d, 5d },
            new double[] { 1d, 1d, 1d, 1d });
    }
}
