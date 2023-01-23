package cn.itcast.wordcount;

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
 * Date 2020/12/26 11:32
 * 将整个 mapreduce 任务 串起来
 * map 和 reduce
 */
public class WordcountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.实现 JOB 实例
        Job job = Job.getInstance(new Configuration(), "wordcount");
        //提交集群的找到类
        job.setJarByClass(WordcountMain.class);
        //2.设置当前输入的格式和输入的文件路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(args[0]));
        //3.设置 map 类和map输出的Key和value类型
        job.setMapperClass(WordcountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //4.设置 shuffle 类 分区 排序 规约 分组，如果没有使用默认即可

        //5.设置 reduce 类和reduce输出的key和value类型
        job.setReducerClass(WordcountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输出的格式和输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(args[1]));
        //7.等待这个任务执行
        boolean flag = job.waitForCompletion(true);
        //8.退出当前程序job
        System.exit(flag?0:1);
    }
}
