package cn.itcast.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 15:32
 * Desc TODO
 */
public class OrderMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "orderJob");
        //输入的类型
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\orders.txt"));
        //map操作
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //shuffle操作
        job.setPartitionerClass(OrderPartition.class);
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        //reduce操作
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day06_mapreduce\\data\\order_output"));
        //等待执行
        boolean flag = job.waitForCompletion(true);
        //程序退出
        System.exit(flag?0:1);
    }
}
