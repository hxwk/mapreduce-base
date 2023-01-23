package cn.itcast.flow2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 17:06
 * Desc TODO
 */
public class Flow2Reduce extends Reducer<Flow2Bean, Text,Text/*手机号  每行数据*/, NullWritable> {
    @Override
    protected void reduce(Flow2Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text tel : values) {
            String result = tel + "\t" + key.toString();
            context.write(new Text(result),NullWritable.get());
        }
    }
}
