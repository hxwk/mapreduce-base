package cn.itcast.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 11:16
 * Desc TODO
 */
public class WordcountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key/*偏移量*/, Text value/*整行数据*/, Context context) throws IOException, InterruptedException {
        //1.读取每行的数据
        String line = value.toString();
        //2.切分数据，根据 , 切分
        if(line!=null||line.length()!=0){
            //3. word,1
            String[] words = line.split(",");
            //4.上下文写出去
            for (String word : words) {
                //发送到下一个阶段
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
}
