package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 通过Configued的getConf() 得到配置信息；
 * Tool接口的run() 方法执行Job。
 */
public class SecondarySortJob extends Configured implements Tool {

    // 返回序列化的对象
    public static LogGenericWritable parseLog(String row) throws ParseException {
        String[] logPart = StringUtils.split(row, "\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);

        LogGenericWritable logData = new LogWritable();
        logData.put("time_tag", new LogFieldWritable(timeTag));
        logData.put("active_name", new LogFieldWritable(activeName));

        for (Map.Entry<String, Object> entry : bizData.entrySet()) {
            logData.put(entry.getKey(), new LogFieldWritable(entry.getValue()));
        }

        return logData;
    }

    public static class LogWritable extends LogGenericWritable {
        protected String[] getFieldNames() {
            return new String[]{"active_name", "session_id", "time_tag", "device_id",
                    "req_url", "user_id", "product_id", "ip", "order_id"};
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, TextLongWritable, LogGenericWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                LogGenericWritable parsedLog = parseLog(value.toString());
                String sessionId = (String) parsedLog.getObject("session_id");
                Long timeTag = (Long) parsedLog.getObject("time_tag");
                TextLongWritable outKey = new TextLongWritable();
                outKey.setText(new Text(sessionId));
                outKey.setCompareValue(new LongWritable(timeTag));
                context.write(outKey, parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    public static class LogReducer extends Reducer<TextLongWritable, LogGenericWritable, NullWritable, Text> {

        private Text sessionId;
        private JSONArray actionPath = new JSONArray();

        public void reduce(TextLongWritable key, Iterable<LogGenericWritable> values, Context context)
                throws IOException, InterruptedException {

            Text sid = key.getText();
            if (sessionId == null || !sessionId.equals(sid)) {
                sessionId = new Text(sid);
                actionPath.clear();
            }

            for (LogGenericWritable v : values) {

                String activeName = (String) v.getObject("active_name");
                String reqUrl = (String) v.getObject("req_url");
                String pathUnit = "pageview".equals(activeName) ? reqUrl : activeName;
                actionPath.add(pathUnit);

                // datum为JSONObject，在本地可以使用JSON进行json字符串和JSONObject之间的转化
                JSONObject datum = JSON.parseObject(v.asJsonString());

                // 每一行日志都加上了一个用户行为路径
                datum.put("action_path", actionPath);
                context.write(null, new Text(datum.toJSONString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config);
        job.setJarByClass(SecondarySortJob.class);
        job.setJobName("parselog");

        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(TextLongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);

        // 设置分组比较器和Partitioner类
        job.setGroupingComparatorClass(TextLongGroupComparator.class);
        job.setPartitionerClass(TextLongPartitioner.class);

        job.setReducerClass(LogReducer.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 如果输出目录存在，删除
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        if (!job.waitForCompletion(true)) {
            throw new RuntimeException(job.getJobName() + " failed");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SecondarySortJob(), args);
        System.exit(res);
    }
}
