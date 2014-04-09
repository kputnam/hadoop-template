package com.github.kputnam.hadoop.demo.implicit.pair;

import com.github.kputnam.hadoop.demo.implicit.Pair;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class TextText extends Pair<Text, Text> {
    public TextText() {
        super();
    }

    public TextText(Text fst, Text snd) {
        super(fst, snd);
    }
}
