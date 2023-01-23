package cn.itcast.friend1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/29 10:44
 * Desc TODO
 */
public class Friend2Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text friend : values) {
            //拼接对应好友列表
            sb.append(friend);
            sb.append("-");
        }
        //好友列表
        sb.deleteCharAt(sb.length() - 1).toString();
        context.write(key,new Text(sb.toString()));
    }
}
