package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.LogBeanWritable;
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

/**
 * 通过Configued的getConf() 得到配置信息；
 * Tool接口的run() 方法执行Job。
 */
public class ParseLogwithToolJob extends Configured implements Tool {

    // 返回序列化的对象
    public static LogBeanWritable parseLog(String row) throws ParseException {
        String[] logPart = StringUtils.split(row, "\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);

        LogBeanWritable logData = new LogBeanWritable();

        logData.setActiveName(activeName);
        logData.setTimeTag(timeTag);
        logData.setIp(bizData.getString("ip"));
        logData.setOrderID(bizData.getString("order_id"));
        logData.setReqUrl(bizData.getString("req_url"));
        logData.setProductID(bizData.getString("product_id"));
        logData.setDeviceID(bizData.getString("device_id"));
        logData.setSessionID(bizData.getString("session_id"));
        logData.setUserID(bizData.getString("user_id"));

        return logData;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogBeanWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                LogBeanWritable parsedLog = parseLog(value.toString());
                context.write(key, parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    public static class LogReducer extends Reducer<LongWritable, LogBeanWritable, NullWritable, Text> {

        public void reduce(LongWritable key, Iterable<LogBeanWritable> values, Context context) throws IOException, InterruptedException {
            for (LogBeanWritable v : values) {
                context.write(null, new Text(v.asJsonString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config);
        job.setJarByClass(ParseLogwithToolJob.class);
        job.setJobName("parselog");

        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogBeanWritable.class);

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
        int res = ToolRunner.run(new Configuration(), new ParseLogwithToolJob(), args);
        System.exit(res);
    }
}
