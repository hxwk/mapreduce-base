package cn.itcast.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 15:16
 * Desc TODO
 */
public class CaipiaoReducer extends Reducer<IntWritable, Text,Text, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,NullWritable.get());
        }
    }
}
