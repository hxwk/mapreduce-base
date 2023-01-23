package cn.itcast.friend1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/29 10:23
 * Desc TODO
 */
public class Friend1Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        // ACD 变成  A-C-D-
        for (Text user : values) {
            buffer.append(user);
            buffer.append("-");
        }
        String userList = buffer.deleteCharAt(buffer.length()-1).toString();
        context.write(new Text(userList),key);
    }
}
