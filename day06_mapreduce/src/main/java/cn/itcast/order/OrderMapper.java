package cn.itcast.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 15:10
 * Desc TODO
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        OrderBean ob = new OrderBean();
        if(line!=null&&line.length()!=0){
            String[] orderArr = line.split("\t");
            ob.setOid(orderArr[0]);
            ob.setPid(orderArr[1]);
            ob.setPrice(Double.parseDouble(orderArr[2]));
        }
        context.write(ob, NullWritable.get());
    }
}
