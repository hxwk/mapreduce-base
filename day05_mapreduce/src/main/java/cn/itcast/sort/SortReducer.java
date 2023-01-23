package cn.itcast.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 16:51
 * Desc TODO
 */
public class SortReducer extends Reducer<SortPojo, NullWritable,SortPojo,NullWritable> {
    @Override
    protected void reduce(SortPojo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key,value);
        }
    }
}
