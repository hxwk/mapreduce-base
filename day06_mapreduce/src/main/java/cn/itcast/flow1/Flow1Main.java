package cn.itcast.flow1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 16:21
 * Desc TODO
 */
public class Flow1Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "flow1job");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\data_flow.dat"));
        job.setMapperClass(Flow1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow1Bean.class);

        job.setReducerClass(Flow1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow1Bean.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\flow1_output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
