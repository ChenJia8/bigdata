package day0903;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TestUpload {

//	@Test
	public void test1() throws Exception{
//		指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");
		
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
//		构造一个输入流，从本地读入数据
		InputStream input = new FileInputStream("f:\\VMs\\hadoop-2.7.3.tar.gz");
		
//		构造一个输出流，指向hdfs
		OutputStream output = client.create(new Path("/folder1/xy.tar.gz"));
		
//		构造一个缓冲区
		byte[] buffer = new byte[1024];
//		长度
		int len = 0;
		
		while((len=input.read(buffer)) > 0){
//			写到输出流中
			output.write(buffer, 0, len);
		}
		
		output.flush();
		
//		关闭客户端
		input.close();
		output.close();
		client.close();
	}

	
	@Test
	public void test2() throws Exception{
//		指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");
		
//		配置参数：拟定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.87.111:9000");
		
//		创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
	
//		构造一个输入流，从本地读入数据
		InputStream input = new FileInputStream("f:\\VMs\\hadoop-2.7.3.tar.gz");
		
//		构造一个输出流，指向hdfs
		OutputStream output = client.create(new Path("/folder1/b.tar.gz"));

//		使用hdfs的一个工具类简化代码
		IOUtils.copyBytes(input, output, 1024);
		
//		关闭客户端
//		client.close();
	}
}
