package cn.itcast.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Author itcast
 * Date 2021/1/3 9:56
 * 1. 创建项目、导入maven依赖（依赖jar包坐标）
 * 2. 创建类，继承 UDF 类
 * 3. 重写方法 evaluate 方法 ，注意输入和输出尽量使用 Hadoop 带序列化的类型
 * 4. 打包 package 上传集群环境
 * 5. 将当前 jar 包移动到 /export/server/hive-2.1.0/lib 目录
 * 6. 添加 jar 到hive环境
 * 7. 注册当前这个自定义函数
 * 8. 使用自定义函数并查看结果
 */
public class MyUDF extends UDF {
    public Text evaluate(final Text data){
        //如果当前的data为空就返回 null
        String str = data.toString();
        if(StringUtils.isNotEmpty(str)){
            //否则转换成大写
            String result = str.toUpperCase();
            return new Text(result);
        }else{
            //如果为空 null
            return null;
        }
    }
}
