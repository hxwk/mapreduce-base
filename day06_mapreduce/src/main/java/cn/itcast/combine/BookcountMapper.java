package cn.itcast.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 10:16
 * Desc TODO
 */
public class BookcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String book = value.toString();
        if (book != null) {
            if (book.contains("入门")) {
                context.write(new Text("计算机"), new IntWritable(1));
            } else if (book.contains("清王朝") || book.contains("史记") || book.contains("天龙八部")) {
                context.write(new Text("历史"), new IntWritable(1));
            } else {
                context.write(new Text("武功秘籍"), new IntWritable(1));
            }
        }
    }
}
