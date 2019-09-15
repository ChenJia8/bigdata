package day0903;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TestDownload {

//	@Test
	public void test2() throws Exception{
//		指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");
		
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个客户端
		FileSystem client = FileSystem.get(conf);
			
//		构造一个输入流，从本地读入数据
		InputStream input = client.open(new Path("/folder1/b.tar.gz"));
		
//		构造一个输出流，指向hdfs
		OutputStream output = new FileOutputStream("f:\\temp\\a.tar.gz");

//		使用hdfs的一个工具类简化代码
		IOUtils.copyBytes(input, output, 1024);
		
//		关闭客户端
//		client.close();
	}

//	获取DataNode的信息（伪分布的环境）
//	@Test
	public void testDataNode() throws Exception{
//		指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");
		
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个hdfs客户端
		DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
		
		DatanodeInfo[] list = fs.getDataNodeStats();
		for (DatanodeInfo info : list) {
			System.out.println(info.getHostName()+"\t"+info.getName());
		}
		
		fs.close();
	}
	
	
	
//	获取数据块的信息
	@Test
	public void testFileBlockLocation() throws Exception{
//		指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");
		
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
//		获取status信息
		FileStatus fileStatus = client.getFileStatus(new Path("/folder1/b.tar.gz"));
		
//		获取文件的数据块信息（数组）
		BlockLocation[] locations = client.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		/*
		 * 伪分布的环境，数据块的冗余度是：1
		 */
		for (BlockLocation blk : locations) {
			System.out.println(Arrays.toString(blk.getHosts())+"\t"+Arrays.toString(blk.getNames()));
		}
		
//		关闭客户端
		client.close();
	}
	
}
