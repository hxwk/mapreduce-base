package cn.itcast.friend1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Author itcast
 * Date 2020/12/29 10:35
 * Desc TODO
 */
public class Friend2Mapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isNotEmpty(line)){
            String[] userFriendArr = line.split("\t");
            //A-C-D
            String users = userFriendArr[0];
            //共同好友 B 取出来
            String friend = userFriendArr[1];
            //将users切分 - 切分成数组 A-C-D  [A,C,D]  [A,C,D]
            String[] userArr = users.split("-");
            Arrays.sort(userArr);
            //将用户列表变成两两之间  A C D
            for(int i=0;i<userArr.length-1;i++){
                for(int j=i+1;j<userArr.length;j++){
                    String user2 = userArr[i]+"-"+userArr[j];
                    context.write(new Text(user2),new Text(friend));
                }
            }
        }
    }
}
