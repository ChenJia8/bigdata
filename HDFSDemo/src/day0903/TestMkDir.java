package day0903;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
/*
 * 错误：Permission denied	针对其他用户，没有W的权限
 * 
 * 有四种方式可以改变hdfs的权限:
 * 
 * 	方式1：设置环境变量HADOOP_USER_NAME=root
 * 
 * 	方式2：通过使用Java的-D参数
 * 		编译：java -Dkey1=vaule123 TestD
 * 		(或者)VM arguments:-ea -DHADOOP_USER_NAME=root
 * 
 * 	方式3：dfs.permissions	--(设置为)-->	false
 * 
 * 	方式4：命令	-chmod	改变hdfs目录的权限
 */
public class TestMkDir {

//	@Test
	public void testMkDir1() throws Exception{
//		指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");
		
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
//		创建目录
		client.mkdirs(new Path("/folder1"));
		
//		关闭客户端
		client.close();
	}
	
	@Test
	public void testMkDir2() throws Exception{
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
//		创建目录
		client.mkdirs(new Path("/folder1"));
		
//		关闭客户端
		client.close();
	}
	
	
}
