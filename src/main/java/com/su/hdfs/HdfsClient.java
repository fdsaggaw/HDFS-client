package com.su.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 */

public class HdfsClient {


    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统

        //创建一个配置文件
        Configuration configuration = new Configuration();
        //连接的集群NameNode地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //获取到的客户端对象
        fs = FileSystem.get(uri, configuration,"suwliang");
    }

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    /**
     * 参数优先级
     * hdfs-default.xml==>hdfs-site.xml==>在项目目录下的配置文件==》代码里面的配置
     */

    @Test
    public void testPut() throws IOException{
        // 上传文件
        fs.copyFromLocalFile(true,
                true,
                new Path("D:\\sunwukong.txt"),
                new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

    @Test
    public void testGet() throws IOException {
        // 下载文件
        fs.copyToLocalFile(false,
                new Path("hdfs://hadoop102/jinguo/weiguo.txt"),
                new Path("D:/"),
                true);
    }

    @Test
    public void testRm() throws IOException {
        // 删除文件
        fs.delete(new Path("/output2"),true);
    }

    @Test
    public void testMv() throws IOException {
        //文件的重命名和移动
        fs.rename(new Path("/si.txt"),new Path("/jinguo/shuguo.txt"));
    }

    @Test
    public void testLs() throws IOException {
        // 查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("==========" + fileStatus.getPath() + "=============");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getOwner());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testIsFile() throws IOException {
        FileStatus[] listStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatus : listStatuses){
            //判断是文件还是文件夹
            if(fileStatus.isFile()){
                System.out.println("f:" + fileStatus.getPath().getName());
            }else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }

    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }

}
