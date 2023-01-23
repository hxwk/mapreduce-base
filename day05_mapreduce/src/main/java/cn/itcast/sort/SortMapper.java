package cn.itcast.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 16:46
 * Desc TODO
 */
public class SortMapper extends Mapper<LongWritable, Text,SortPojo, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取每行的数据
        String line = value.toString();
        //初始化 SortPojo
        SortPojo sortPojo = new SortPojo();
        //2.切割每行数据封装成 SortPojo 对象
        if(line!=null&&line.length()!=0){
            String[] wordNum = line.split("\t");
            sortPojo.setWord(wordNum[0]);
            sortPojo.setNum(Integer.parseInt(wordNum[1]));
            //3.通过上下文写出去
            context.write(sortPojo,NullWritable.get());
        }

    }
}
