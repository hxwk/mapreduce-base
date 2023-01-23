package cn.itcast.flow2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 17:10
 * Desc TODO
 */
public class Flow2Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flow2Job");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\flow1_output\\part-r-00000"));
        job.setMapperClass(Flow2Mapper.class);
        job.setMapOutputKeyClass(Flow2Bean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Flow2Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\flow2_output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
