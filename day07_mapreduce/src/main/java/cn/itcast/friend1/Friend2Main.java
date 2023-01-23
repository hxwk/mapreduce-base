package cn.itcast.friend1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/29 10:47
 * Desc TODO
 */
public class Friend2Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设置配置文件
        Configuration conf = new Configuration();
        //设置环形缓冲区最大的值
        conf.set("mapreduce.task.io.sort.mb","200");
        conf.set("mapreduce.map.sort.spill.percent","0.9");
        conf.set("mapreduce.task.io.sort.factor","100");

        Job job = Job.getInstance(conf, "friend2job");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\project\\Shanghai26\\day07_mapreduce\\data\\friend1_output\\part-r-00000"));
        job.setMapperClass(Friend2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Friend2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\project\\Shanghai26\\day07_mapreduce\\data\\friend2_output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
