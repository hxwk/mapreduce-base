package cn.itcast.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 10:27
 * Desc TODO
 */
public class BookcountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "bookcountJob");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\combiner.txt"));
        job.setMapperClass(BookcountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //shuffle 阶段的自定义 combine操作
        job.setCombinerClass(BookcountCombiner.class);
        job.setReducerClass(BookcountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\combine_output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
