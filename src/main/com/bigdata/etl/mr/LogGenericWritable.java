package com.bigdata.etl.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class LogGenericWritable implements Writable {
    private LogFieldWritable[] datum;
    private String[] names;
    private Map<String, Integer> namesIndex;

    // 不同的日志格式写入自己的字段名
    abstract protected String[] getFieldNames();

    public LogGenericWritable() {
        names = getFieldNames();
        if (names == null) {
            throw new RuntimeException("field names cannot be null!");
        }
        namesIndex = new HashMap<String, Integer>();
        for (int index = 0; index < names.length; index++) {
            if (namesIndex.containsKey(names[index])) {
                throw new RuntimeException("The field " + names[index] + " duplicate");
            }
            namesIndex.put(names[index], index);
        }
        // 初始化 datum
        datum = new LogFieldWritable[names.length];
        for (int index = 0; index < datum.length; index++) {
            datum[index] = new LogFieldWritable();
        }
    }


    // 1.保证fieldNames和datum对应元素的下标相同
    public void put(String name, LogFieldWritable value) {
        int index = getIndexWithName(name);
        datum[index] = value;
    }

    private int getIndexWithName(String name) {
        Integer index = namesIndex.get(name);
        if (index == null) {
            throw new RuntimeException("The field " + name + " not registered");
        }
        return index;
    }

    // 2.
    public LogFieldWritable getWritable(String name) {
        int index = getIndexWithName(name);
        return datum[index];
    }

    // 2.
    public Object getObject(String name) {
        return getWritable(name).getObject();
    }

    // 3.
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, names.length);
        for (int i = 0; i < names.length; i++) {
            datum[i].write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        datum = new LogFieldWritable[length];
        for (int i = 0; i < length; i++) {
            LogFieldWritable value = new LogFieldWritable();
            value.readFields(in);
            datum[i] = value;
        }
    }

    // reduce阶段将日志文件的每一行都转化为String格式。
    public String asJsonString() {
        JSONObject json = new JSONObject();
        for (int i = 0; i < names.length; i++) {
            json.put(names[i], datum[i].getObject());
        }

        return json.toJSONString();
    }

}
