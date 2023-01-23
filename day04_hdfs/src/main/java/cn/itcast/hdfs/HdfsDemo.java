package cn.itcast.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Author itcast
 * Date 2020/12/25 10:14
 * 需求：
 * 1.获取文件系统 （Configuration  FileSystem）
 * 2.遍历HDFS中所有的文件
 * 3.HDFS上创建文件夹
 * 4.下载文件 本地
 * 5.上传文件 从本地上传文件系统
 * 6.小文件合并，从本地的文件夹中将小文件合并并上传到 hdfs
 * 7.hdfs访问权限的控制
 */
public class HdfsDemo {
    //1.创建方法 getFileSystem1
    //2.初始化 Configuration 配置类
    //3.设置配置属性 fs.defaultFS 指定为 hdfs://node1:8020
    //4.获取指定的文件系统
    //打印输出
    FileSystem fileSystem = null;

    @Before
    public void getFileSystem1() throws URISyntaxException, IOException, InterruptedException {
        //抽象类怎么实例化
        //1.先找到内部系统已经默认实现的子类
        //2.工厂类
        //3.静态的实例
        //第一种获取 实例化FileSystem的子类进行实例化
        //FileSystem fileSystem = new DistributedFileSystem();
        //第二种获取FileSystem
        // URI 分布式文件系统：HDFS:// 本地文件：file://
        // "hdfs://node1:8020"
        // Configuration 初始化的时候会 静态代码块中加载 resource下的 core-site.xml 和 core-default.xml
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(new URI(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY))
                , conf
                , "root"); // 模拟用户 kerboese 认证
        //fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),conf,"root");

    }

    //遍历HDFS中所有文件
    //1.创建 listMyFiles 方法
    //2.创建 FileSystem 对象
    //3.通过listFiles方法获取 RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
    //4.遍历文件夹并打印路径
    //关闭文件系统
    public void listMyFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem
                .listFiles(new Path("/config"), false);
        while (listFiles.hasNext()) {
            System.out.println(listFiles.next().getPath());
        }
    }

    //1.创建mkdirs方法用于创建文件夹
    //2.获取文件系统
    //创建文件夹
    //格式化输出 %s 是字符串输出 %d; 是数字输出; %f 是double类型输出
    // 创建文件 fileSystem.create(new Path(创建文件的路径))
    // 删除文件 fileSystem.delete(文件,true) true:递归删除
    public void mkdirs() throws IOException {
        String dir = "/config2";
        boolean flag = fileSystem.mkdirs(new Path(dir));
        if (flag) {
            System.out.printf("当前文件夹 %s 创建成功", dir);
        }
    }

    //1.创建 getFileToLocal 方法
    //2.创建文件系统 FileSystem
    //3.指定打开要下载的文件路径
    //4.创建文件输出流对象，指定好下载的完整文件名
    //5.借助IOUtils 工具类将输入流输出
    //6.关闭输入流和输出流
    //关闭文件系统
    public void getFileToLocal() throws IOException {
        FSDataInputStream fis = fileSystem.open(new Path("/config/core-site.xml"));
        FileOutputStream fos = new FileOutputStream("D:\\project\\Shanghai26\\day04_hdfs\\data\\config.xml");
        IOUtils.copy(fis, fos);
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
    }

    //1.创建putData 方法
    //2.获取文件系统的对象
    //3.通过 copyFromLocalFile 从本地文件拷贝到指定的HDFS文件
    //关闭文件系统
    public void putData() throws IOException {
        fileSystem.copyFromLocalFile(false, true,
                new Path("D:\\project\\Shanghai26\\day04_hdfs\\data\\config.xml"),
                new Path("/config2/core-site.xml"));
    }

    //需求：将本地的某个文件夹下的小文件列表遍历写到HDFS文件系统的某个文件中。
    //1.创建 mergeFile 方法
    //2.获取以root用户操作的文件系统
    //3.在根路径下创建文件 比如 bigfile.txt
    //4.创建获取本地文件系统
    //5.通过本地文件系统 listStatus 获取文件列表，为一个元数据集合
    //6.遍历文件集合
    //7.通过本地文件系统打开文件的文件流
    //8.通过IO工具类拷贝数据流
    //9.关闭输入流
    //退出循环后关闭输出流和关闭本地文件系统，关闭文件系统
    public void mergeFile() throws IOException {
        //在hdfs上创建一个路径，用于将本地文件的内容拷贝到此文件中 fos
        FSDataOutputStream fos = fileSystem.create(new Path("/bigfile2.txt"));
        //创建本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //读取本地的所有文件
        FileStatus[] fileList = local
                .listStatus(new Path("D:\\project\\Shanghai26\\day04_hdfs\\data"));
        FSDataInputStream fis = null;
        //遍历读取到的所有文件路径
        for (FileStatus fileStatus : fileList) {
            //通过元数据获取当前的文件的路径
            Path path = fileStatus.getPath();
            //打开当前读取到的文件内容
            fis = local.open(path);
            //将读出来的数据流拷贝给输出流 bigfile.txt
            IOUtils.copy(fis, fos);
        }
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
    }

    //1.创建方法getConfig用于下载文件core-site.xml到本地路径下
    //2.通过将hdfs上文件 copyToLocalFile到本地，测试是否有权限影响。
    //模拟当前用户下载文件 core-site.xml
    //解决读取权限问题的办法：
    //1.直接修改文件的权限  hdfs dfs -chmod 766
    //2.直接模拟用户使用 root 宿主用户进行访问
    @Test
    public void getConfig() throws IOException {
        fileSystem
                .copyToLocalFile(new Path("/config/core-site.xml"),
                new Path("D:\\project\\Shanghai26\\day04_hdfs\\data\\core-site.xml"));
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

}
