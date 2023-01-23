package cn.itcast.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;

/**
 * Author itcast
 * Date 2020/12/20 16:46
 * 需求：
 * 1.创建一个节点 hive 内容 hive
 * 2.修改这个节点的内容 /hive hive -> hadoop
 * 3.判断如果当前的节点/hive存在，读取/hive下的数据
 * 4.创建一个带监听/hive的事件，捕捉当前这个节点做了什么操作
 */
public class ZookeeperDemo {
    public CuratorFramework client;

    @Before
    public void init() {
        //连接字符串
        String connectString = "node1:2181,node2:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(2000, 3);
        //创建一个客户端 CuratorFrameworkFactory
        client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        //开启这个客户端
        client.start();
    }

    //1.创建 Znode 节点，添加 createZnode 方法
    //1.1 定制一个重试策略
    //1.2 工厂类获取一个客户端对象
    //1.3 开启一个客户端对象
    //1.4 创建Znode 节点对象，在节点/hello2下写入一个 world 内容
    //1.5 关闭客户端对象

    public void createZnode() throws Exception {
        //写入数据 /hello2 world
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2", "world".getBytes());
    }

    //2.修改 Znode 节点数据，创建 nodeData 方法
    //2.1 定制一个重试策略
    //2.2 工厂类获取一个客户端对象
    //2.3 开启一个客户端对象
    //2.4 客户端设置指定路径下的数据
    //2.5 关闭客户端对象

    public void nodeData() throws Exception {
        Stat stat = client.setData().forPath("/hello2", "hive".getBytes());
        long ctime = stat.getCtime();
        long mtime = stat.getMtime();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String ctimeStr = dateFormat.format(ctime);
        String mtimeStr = dateFormat.format(mtime);
        System.out.println(ctimeStr);
        System.out.println(mtimeStr);
    }

    //3.Znode 节点的数据查询 getData方法
    //3.1 定制一个重试策略
    //3.2 工厂类获取一个客户端对象
    //3.3 开启一个客户端对象
    //3.4 判断路径并获取 /hello2 的数据
    //3.4.1 实例化 ZkClient
    //3.4.2 通过zkClient判断路径是否存在，如果存在了获取此节点下的值
    //3.4.3 如果不存在路径提示，路径不存在
    //3.5 打印输出此路径下的数据
    //3.6 关闭客户端对象
    public void getData() throws Exception {
        //判断节点是否存在 如果存在在获取数据
        ZkClient zkClient = new ZkClient("node1:2181");
        if (zkClient.exists("/hello2")) {
            byte[] value = client.getData().forPath("/hello2");
            String values = new String(value);
            System.out.println(values);
        }
    }

    //4.创建监听节点事件 watchNode方法
    //4.1 定制一个重试策略
    //4.2 工厂创建客户端对象
    //4.3 开启客户端对象
    //4.4 将监听的节点树指定给缓存TreeCache中
    //4.5 自定义监听事件，实现匿名内部类，判断事件类型
    //4.6 开启缓存树进程
    //4.7 阻塞进程
    @Test
    public void watchNode() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/hello2");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                //如果是创建节点 打印当前节点被创建
                //如果当前节点被修改 被修改
                //如果当前节点被删除 被删除
                switch (event.getType()) {
                    case NODE_ADDED:
                        System.out.println("当前节点被创建");
                        break;
                    case NODE_UPDATED:
                        System.out.println("当前节点被更新");
                        break;
                    case NODE_REMOVED:
                        System.out.println("当前节点被删除");
                        //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("")
                }
            }
        });
        treeCache.start();
        //Thread.sleep(100000);
    }

    @After
    public void close() {
        //客户端关闭
        client.close();
    }
}
