package cn.itcast.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 15:05
 * Desc TODO
 */
public class CaipiaoMapper extends Mapper<LongWritable, Text, IntWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("每行的偏移量: "+key.get());
        String line = value.toString();
        if(line!=null&&line.length()!=0){
            String[] arrs = line.split("\t");
            int caipiaoCode = Integer.parseInt(arrs[5]);
            context.write(new IntWritable(caipiaoCode),value);
        }
    }
}
