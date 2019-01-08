package com.bigdata.etl.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextLongWritable implements WritableComparable<TextLongWritable> {

    private Text text;
    private LongWritable compareValue;

    public TextLongWritable() {
        this.text = new Text();
        this.compareValue = new LongWritable(0);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        // 尽可能的保证hashCode相等的两个元素的text和compareValue相同
        return this.text.hashCode() * prime + this.compareValue.hashCode();
    }

    public int compareTo(TextLongWritable o) {
        int result = this.text.compareTo(o.getText());
        if (result == 0) {
            result = this.compareValue.compareTo(o.getCompareValue());
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        this.text.write(out);
        WritableUtils.writeVLong(out, this.compareValue.get());
    }

    public void readFields(DataInput in) throws IOException {
        this.text.readFields(in);
        this.compareValue.set(WritableUtils.readVLong(in));
    }

    public Text getText() {
        return text;
    }

    public void setText(Text text) {
        this.text = text;
    }

    public LongWritable getCompareValue() {
        return compareValue;
    }

    public void setCompareValue(LongWritable compareValue) {
        this.compareValue = compareValue;
    }
}
