package cn.itcast.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 15:18
 * Desc TODO
 */
public class CaipiaoMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.实现一个 job任务 的实例
        Job job = Job.getInstance(new Configuration(), "caipiaoJob");
        //2.设置对应输入的格式和文本路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day05_mapreduce\\data\\partition.csv"));
        //3.设置map类和map输出的key value的类型
        job.setMapperClass(CaipiaoMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4.设置 shuffle 分区自定义类
        job.setPartitionerClass(MyPartition.class);
        //5.设置reduce类和reduce输出的key value的类型
        job.setReducerClass(CaipiaoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //6.设置输出的格式和文本输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day05_mapreduce\\data\\p_output"));
        //7.设置当前的reducer的任务数，分区数要和reduce的个数一样
        job.setNumReduceTasks(2);
        //8.等待任务执行
        boolean flag = job.waitForCompletion(true);
        //9.程序退出
        System.exit(flag?0:1);
    }
}
