package day0912.partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyPartitionerReducer extends Reducer<IntWritable, Emp, IntWritable, Emp> {

	@Override
	protected void reduce(IntWritable k3, Iterable<Emp> v3,Context context) throws IOException, InterruptedException {
//		直接输出
		for (Emp e : v3) {
			context.write(k3, e);
		}
	}
	
}
