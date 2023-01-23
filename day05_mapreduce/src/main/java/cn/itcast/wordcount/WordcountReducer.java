package cn.itcast.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 11:25
 * Desc TODO
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable wordNum : values) {
            count += wordNum.get();
        }
        context.write(key, new IntWritable(count));
    }
}
