package day0919.mrunit;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;


public class WordCountUnitTest {

	@Test
	public void testMapper() throws Exception{
/*		java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
 * 		设置环境变量就可以，用到 Hadoop 在 windows 上的安装包
 */	
		System.setProperty("hadoop.home.dir","F:\\VMs\\hadoop-2.7.3");
		
//		创建一个Map 对象：测试对象
		WordCountMapper mapper = new WordCountMapper();
		
//		创建一个MapDriver 进行单元测试
		MapDriver<LongWritable,Text,Text,IntWritable> driver = new MapDriver<>(mapper);
		
//		指定map的输入：		k1					v1
		driver.withInput(new LongWritable(1), new Text("I love Beijing"));
		
//		指定map的输出：		k2				v2 --> 我们期望得到结果
		driver.withOutput(new Text("I"), new IntWritable(1))
			  .withOutput(new Text("love"), new IntWritable(1))
			  .withOutput(new Text("Beijing"), new IntWritable(1));
		
//		执行单元测试：对比 期望的结果 和 实际的结果
		driver.runTest();
		
	}
	
	
	@Test
	public void testReducer() throws Exception{
		System.setProperty("hadoop.home.dir","F:\\VMs\\hadoop-2.7.3");

//		创建一个测试对象
		WordCountReducer reducer = new WordCountReducer();
		
//		创建一个Driver
		ReduceDriver<Text,IntWritable,Text,IntWritable> driver = new ReduceDriver<>(reducer);
		
//		指定 Driver 的输入：k3	v3
//		构造一下 v3 是一个集合
		List<IntWritable> v3 = new ArrayList<>();
		v3.add(new IntWritable(1));
		v3.add(new IntWritable(1));
		v3.add(new IntWritable(1));
		
		driver.withInput(new Text("Beijing"), v3);
		
//		指定输出的数据	指定	k4	v4
		driver.withOutput(new Text("Beijing"), new IntWritable(3));		
		
//		执行单元测试
		driver.runTest();		
		
	}
	
	
	@Test
	public void testJob() throws Exception{
		System.setProperty("hadoop.home.dir","F:\\VMs\\hadoop-2.7.3");
		
//		创建测试的对象
		WordCountMapper mapper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();
		
//		创建一个Driver
//		MapReducerDriver<k1,v1,k2,v2,k4,v4>
		MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>
			driver = new MapReduceDriver<>();
		
//		指定Map的输入
		driver.withInput(new LongWritable(1), new Text("I love Beijing"))
			  .withInput(new LongWritable(4), new Text("I love China"))
			  .withInput(new LongWritable(7), new Text("Beijing is the capital of China"));
		
//		指定Reducer的输出
		driver.withOutput(new Text("Beijing"), new IntWritable(2))
			  .withOutput(new Text("China"), new IntWritable(2))
			  .withOutput(new Text("I"), new IntWritable(2))
			  .withOutput(new Text("capital"), new IntWritable(1))
			  .withOutput(new Text("is"), new IntWritable(1))
			  .withOutput(new Text("love"), new IntWritable(2))
			  .withOutput(new Text("of"), new IntWritable(1))
			  .withOutput(new Text("the"), new IntWritable(1));
		
//		执行单元测试
		driver.runTest();
	}
}
