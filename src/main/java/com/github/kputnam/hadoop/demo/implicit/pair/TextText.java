package com.github.kputnam.hadoop.demo.implicit.pair;

import com.github.kputnam.hadoop.demo.implicit.Pair;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class TextText extends Pair<Text, Text> {
    public static TextText of(Text fst, Text snd) {
        return new TextText(fst, snd);
    }

    public TextText() {
        super();
    }

    public TextText(Text fst, Text snd) {
        super(fst, snd);
    }
}
