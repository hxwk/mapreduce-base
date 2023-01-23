package cn.itcast.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 16:52
 * Desc TODO
 */
public class SortMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建一个 job 对象
        Job job = Job.getInstance(new Configuration(), "sortJob");
        //2.设置当前输入的格式和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day05_mapreduce\\data\\sort.txt"));
        //3.设置map类和map输出的key value类型
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SortPojo.class);
        job.setMapOutputValueClass(NullWritable.class);
        //4.设置shuffle 分区 排序 规约 分组
        //5.设置 reduce类和reduce 输出的 key value类型
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(SortPojo.class);
        job.setOutputValueClass(NullWritable.class);
        //6.设置输出的格式和输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day05_mapreduce\\data\\sort_output"));
        //7.等待当前job执行
        boolean flag = job.waitForCompletion(true);
        //8.程序退出
        System.exit(flag?0:1);
    }
}
