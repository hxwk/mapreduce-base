package cn.itcast.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Author itcast
 * Date 2020/12/28 15:13
 * Desc TODO
 */
public class OrderPartition extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        //orderBean.getOid() 拿出来是当前订单号 Order_0000001
        //.hashCode() 整数，有可能是整数，有可能是负数
        // & Integer.MAX_VALUE 取出来所有的在正整数范围内的值
        // % numPartitions 取出来分区数
        return (orderBean.getOid().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
