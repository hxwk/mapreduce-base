package cn.itcast.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Author itcast
 * Date 2020/12/26 15:12
 * Desc TODO
 */
public class MyPartition extends Partitioner<IntWritable, Text> {
    /*
    * 返回 int 就是相同 k2 的标记
     */
    @Override
    public int getPartition(IntWritable caipiaoCode, Text text, int numPartition) {
        if(caipiaoCode.get()>=15){
            return 0;
        }else{
            return 1;
        }
    }
}
