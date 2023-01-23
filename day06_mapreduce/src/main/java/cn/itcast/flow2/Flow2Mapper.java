package cn.itcast.flow2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 17:03
 * 注意：
 * 生成 k2 必须是要排序的类
 */
public class Flow2Mapper extends Mapper<LongWritable, Text, Flow2Bean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Flow2Bean flow2Bean = new Flow2Bean();
        if (StringUtils.isNotEmpty(line)) {
            String[] flow2 = line.split("\t");
            String tel = flow2[0];
            flow2Bean.setUpPackNum(Long.parseLong(flow2[1]));
            flow2Bean.setDownPackNum(Long.parseLong(flow2[2]));
            flow2Bean.setUpPayLoad(Long.parseLong(flow2[3]));
            flow2Bean.setDownPayLoad(Long.parseLong(flow2[4]));
            context.write(flow2Bean, new Text(tel));
        }
    }
}
