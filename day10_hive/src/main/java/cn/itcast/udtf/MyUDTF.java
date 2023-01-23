package cn.itcast.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * Author itcast
 * Date 2021/1/3 10:21
 * 1.创建一个自定义类继承 GenericUDTF
 * 2.重写 initialize 方法
 * 2.1.初始化列名数组
 * 2.2.设置列名
 * 2.3.初始化对象检查器数组
 * 2.4.设置输出的列的值类型为原子对象检查 java字符串对象类型
 * 2.5.返回对象检查工厂获取的标准结构对象检查器
 * 3.重写 process 方法
 * 3.1.获取args输入数据
 * 3.2.获取args分隔符
 * 3.3.将原始数据按照传入的分隔符进行切分
 * 遍历切分后的结果，并赋值给长度为1的对象数组，最后 forward 输出对象数组
 */
public class MyUDTF extends GenericUDTF {
    private final transient String[] forwordArr = new String[1];
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //2.1.初始化列名数组
        ArrayList<String> fieldNameArr = new ArrayList<>();
        //2.2.设置列名(注意)
        fieldNameArr.add("col");
        //2.3.初始化对象检查器数组
        ArrayList<ObjectInspector> objectInspectors = new ArrayList<>();
        //2.4.设置输出的列的值类型为原子对象检查 java字（注意）
        objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //2.5.返回对象检查工厂获取的标准结构对象检查器
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNameArr,objectInspectors);
    }

    // select myUDTF('hello,world,hadoop,flink',',')
    @Override
    public void process(Object[] args) throws HiveException {
        // 3.1.获取args输入数据
        String data = args[0].toString();
        // 3.2.获取args分隔符
        String seperate = args[1].toString();
        // 3.3.将原始数据按照传入的分隔符进行切分
        String[] wordArr = data.split(seperate);
        // 3.4.遍历切分后的结果，并赋值给长度为1的对象数组，最后 forward 输出对象数组
        for (String word : wordArr) {
            forwordArr[0] = word;
            forward(forwordArr);
        }
    }

    @Override
    public void close() throws HiveException {
    }
}
