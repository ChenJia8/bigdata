package day0912.sort.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpSortMapper extends Mapper<LongWritable, Text, Emp, NullWritable> {

	@Override
	protected void map(LongWritable key1, Text value1,Context context)
			throws IOException, InterruptedException {
//	数据
		String data = value1.toString();
		
//	分词
		String[] words = data.split(",");
		
//	生成员工对象
		Emp emp = new Emp();
		emp.setEmpno(Integer.parseInt(words[0]));
		emp.setEname(words[1]);
		emp.setJob(words[2]);
		emp.setMgr(Integer.parseInt(words[3]));
		emp.setHiredate(words[4]);
		emp.setSal(Integer.parseInt(words[5]));
		emp.setComm(Integer.parseInt(words[6]));
		emp.setDeptno(Integer.parseInt(words[7]));
		
//	输出员工对象： k2 员工对象		v2	空值
		context.write(emp, NullWritable.get());
		
	}

	
}
