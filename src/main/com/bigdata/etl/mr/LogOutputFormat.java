package com.bigdata.etl.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// 尖括号中的每个元素都指代一种未知元素。e.g：日志解析的K 为Text，V 为Text
public class LogOutputFormat<K, V> extends TextOutputFormat<K, V> {
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    private RecordWriter writer = null;

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        if (writer == null) {
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));
        }

        return writer;
    }

    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path taskOutputPath;
        OutputCommitter committer = getOutputCommitter(conf);
        if (committer instanceof FileOutputCommitter) {
            taskOutputPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("Undefined job output path");
            }
            taskOutputPath = outputPath;
        }

        return taskOutputPath;
    }

    public class MultiRecordWriter extends RecordWriter<K, V> {
        private Map<String, RecordWriter<K, V>> recordWriters;
        private TaskAttemptContext job;
        private Path outputPath;

        public MultiRecordWriter(TaskAttemptContext job, Path outputPath) {
            super();
            this.job = job;
            this.recordWriters = new HashMap<String, RecordWriter<K, V>>();
            this.outputPath = outputPath;
        }

        // key传入的为文件名的前半部分
        public void write(K key, V value) throws IOException, InterruptedException {
            TaskID taskID = job.getTaskAttemptID().getTaskID();
            int partition = taskID.getId();
            String baseName = getFileBaseName(key, NUMBER_FORMAT.format(partition));
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);
            if (rw == null) {
                // 通过baseName获得相应的RecordWriter，将Reduce的输出结果输出到HDFS
                rw = getBaseRecorderWriter(job, baseName);
                this.recordWriters.put(baseName, rw);
            }

            rw.write(null, value);
        }

        private RecordWriter<K, V> getBaseRecorderWriter(TaskAttemptContext job, String baseName) throws IOException {
            RecordWriter<K, V> recordWriter;
            boolean isCompressioned = getCompressOutput(job);
            Configuration conf = job.getConfiguration();
            if (isCompressioned) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
                Path file = new Path(outputPath, baseName + codec.getDefaultExtension());
                FSDataOutputStream codecOut = file.getFileSystem(conf).create(file, false);
                recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(codecOut)));
            } else {
                Path file = new Path(outputPath, baseName);
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
                recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(fileOut));
            }

            return recordWriter;
        }

        // key指定为Text类型，具体为part
        private String getFileBaseName(K key, String name) {
            // e.g: part-00000
            return new StringBuilder(60).append(key.toString()).append("-").append(name).toString();
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();
            while (values.hasNext()) {
                values.next().close(context);
            }
            this.recordWriters.clear();
        }
    }
}
