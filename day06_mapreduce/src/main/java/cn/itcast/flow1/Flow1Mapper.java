package cn.itcast.flow1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 16:10
 * Desc TODO
 */
public class Flow1Mapper extends Mapper<LongWritable, Text,Text,Flow1Bean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Flow1Bean flow1Bean = new Flow1Bean();
        //StringUtils 工具类 ，用于判断当前字符串是否为空
        if(StringUtils.isNotEmpty(line)){
            String[] flowArr = line.split("\t");
            flow1Bean.setUpPackNum(Long.parseLong(flowArr[6]));
            flow1Bean.setDownPackNum(Long.parseLong(flowArr[7]));
            flow1Bean.setUpPayLoad(Long.parseLong(flowArr[8]));
            flow1Bean.setDownPayLoad(Long.parseLong(flowArr[9]));
            String tel = flowArr[1];
            context.write(new Text(tel),flow1Bean);
        }
    }
}
