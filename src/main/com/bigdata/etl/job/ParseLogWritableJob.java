package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.LogBeanWritable;
import com.bigdata.etl.mr.LogFieldWritable;
import com.bigdata.etl.mr.LogGenericWritable;
import com.bigdata.utils.IPUtil;
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
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 通过Configued的getConf() 得到配置信息；
 * Tool接口的run() 方法执行Job。
 */
public class ParseLogWritableJob extends Configured implements Tool {

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

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogGenericWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                LogGenericWritable parsedLog = parseLog(value.toString());
                context.write(key, parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    public static class LogReducer extends Reducer<LongWritable, LogGenericWritable, NullWritable, Text> {

        public void setup(Context context) throws IOException {
            // 将HDFS上的IP地址库拉取到本地，将IP地址库文件加载到内存
/*            Configuration config = context.getConfiguration();
            FileSystem fs = FileSystem.get(config);
            Path ipFile = new Path(config.get("ip.file.path"));
            Path localPath = new Path(this.getClass().getResource("/").getPath());
            fs.copyToLocalFile(ipFile, localPath);*/

            // 可以通过分布式缓存直接分发给各个节点
            IPUtil.load("17monipdb.dat");
        }

        public void reduce(LongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException {
            for (LogGenericWritable v : values) {
                String ip = (String) v.getObject("ip");
                String[] address = IPUtil.find(ip);
                JSONObject addrObj = new JSONObject();
                addrObj.put("country", address[0]);
                addrObj.put("province", address[1]);
                addrObj.put("city", address[2]);

                JSONObject datum = JSON.parseObject(v.asJsonString());
                datum.put("address", addrObj);

                context.write(null, new Text(datum.toJSONString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        // 继承Configured类的原因
        Configuration config = getConf();
        // 设置自定义参数
        config.addResource("mr.xml");

        Job job = Job.getInstance(config);
        job.setJarByClass(ParseLogWritableJob.class);
        job.setJobName("parselog");
        job.addCacheFile(new URI(config.get("ip.file.path")));

        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);

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
        int res = ToolRunner.run(new Configuration(), new ParseLogWritableJob(), args);
        System.exit(res);
    }
}
