package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.*;
import com.bigdata.utils.IPUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 通过Configued的getConf() 得到配置信息；
 * Tool接口的run() 方法执行Job。
 */
public class MultiOutputJob extends Configured implements Tool {

    // 返回序列化的对象
    public static LogGenericWritable parseLog(String row) throws Exception {
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
                    "req_url", "user_id", "product_id", "ip", "order_id",
                    "error_flag", "error_log"};
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, TextLongWritable, LogGenericWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Counter errorCounter = context.getCounter("Log Error", "Parse Error");
            try {
                LogGenericWritable parsedLog = parseLog(value.toString());
                String sessionId = (String) parsedLog.getObject("session_id");
                Long timeTag = (Long) parsedLog.getObject("time_tag");
                TextLongWritable outKey = new TextLongWritable();
                outKey.setText(new Text(sessionId));
                outKey.setCompareValue(new LongWritable(timeTag));
                context.write(outKey, parsedLog);
            } catch (Exception e) {
                errorCounter.increment(1);
                // 解析日志异常
                LogGenericWritable v = new LogWritable();
                v.put("error_flag", new LogFieldWritable("error"));
                v.put("error_log", new LogFieldWritable(value.toString()));
                TextLongWritable outKey = new TextLongWritable();
                // 使得异常日志分散到不同reduce节点
                int randKey = (int) (Math.random() * 100);
                outKey.setText(new Text("error" + randKey));
                context.write(outKey, v);
            }
        }

    }

    public static class LogReducer extends Reducer<TextLongWritable, LogGenericWritable, Text, Text> {

        private Text sessionId;
        private JSONArray actionPath = new JSONArray();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 可以通过分布式缓存直接分发给各个节点
            IPUtil.load("17monipdb.dat");
        }

        public void reduce(TextLongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException {

            Text sid = key.getText();
            if (sessionId == null || !sessionId.equals(sid)) {
                sessionId = new Text(sid);
                actionPath.clear();
            }

            for (LogGenericWritable v : values) {
                // datum为JSONObject，在本地可以使用JSON进行json字符串和JSONObject之间的转化
                JSONObject datum = JSON.parseObject(v.asJsonString());
                if (v.getObject("error_flag") == null) {
                    String ip = (String) v.getObject("ip");
                    String[] address = IPUtil.find(ip);
                    JSONObject addrObj = new JSONObject();
                    addrObj.put("country", address[0]);
                    addrObj.put("province", address[1]);
                    addrObj.put("city", address[2]);
                    // 不用addrObj.toJSONString，否则会出现\" 的符号
                    datum.put("address", addrObj);

                    String activeName = (String) v.getObject("active_name");
                    String reqUrl = (String) v.getObject("req_url");
                    String pathUnit = "pageview".equals(activeName) ? reqUrl : activeName;
                    actionPath.add(pathUnit);
                    // 每一行日志都加上了一个用户行为路径
                    datum.put("action_path", actionPath);
                }

                String outputKey = v.getObject("error_flag") == null ? "part" : "error/part";
                context.write(new Text(outputKey), new Text(datum.toJSONString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        // 设置自定义参数
        config.addResource("mr.xml");
        Job job = Job.getInstance(config);
        job.setJarByClass(MultiOutputJob.class);
        job.setJobName("parselog");
        job.addCacheFile(new URI(config.get("ip.file.path")));

        // 输入 --> Map
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(TextLongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);

        // Shuffle过程
        // 设置分组比较器和Partitioner类
        job.setGroupingComparatorClass(TextLongGroupComparator.class);
        job.setPartitionerClass(TextLongPartitioner.class);

        // Reduce --> 输出
        job.setReducerClass(LogReducer.class);
        job.setOutputValueClass(Text.class);
        // 设置OutputFormatClass，将日志分为正常日志和异常日志分别存储
        job.setOutputFormatClass(LogOutputFormat.class);

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
        int res = ToolRunner.run(new Configuration(), new MultiOutputJob(), args);
        System.exit(res);
    }
}
