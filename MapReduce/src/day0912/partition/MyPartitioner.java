package day0912.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<IntWritable, Emp> {

	//	numTask:分区的个数
	@Override
	public int getPartition(IntWritable k2, Emp v2, int numTask) {
		//	建立我们的分区规则
		//	得到该员工的部门号
		int deptno = v2.getDeptno();

		if(deptno == 10){
//			放入1号分区
			return 1%numTask;
		}else if(deptno == 20){
//			放入2号分区
			return 2%numTask;
		}else{
//			30号部门，放入0号分区
			return 3%numTask;
		}
	}

}
