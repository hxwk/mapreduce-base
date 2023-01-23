package cn.itcast.friend1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/29 10:19
 * Desc TODO
 */
public class Friend1Mapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //判断空
        if(StringUtils.isNotEmpty(line)){
            String[] userFriends = line.split(":");
            String user = userFriends[0];
            String friends = userFriends[1];
            //将好友 friends 进行切分得到好友数组
            String[] friendArr = friends.split(",");
            for (String friend : friendArr) {
                context.write(new Text(friend),new Text(user));
            }
        }
    }
}
