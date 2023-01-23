package cn.itcast.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Author itcast
 * Date 2020/12/28 15:18
 * Desc TODO
 */
public class OrderGroupingComparator extends WritableComparator {

    //调用父类的构造方法
    //传递了两个值，一个是 OrderBean.class 给分组组件注册了一个OrderBean
    //传递第二个 true,当前这个 OrderBean 允许被实例化。
    public OrderGroupingComparator(){
        super(OrderBean.class,true);
    }

    //重写compare方法，用于判断当前的两个对象的字段是否相同，如果相同就放到一个分组中
    //自定义分组，根据 OrderId
    //分组规则： 如果两个相等就会分到一个组。
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        return first.getOid().compareTo(second.getOid());
    }
}
