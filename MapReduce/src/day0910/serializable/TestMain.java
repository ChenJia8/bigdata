package day0910.serializable;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class TestMain {

	public static void main(String[] args) throws Exception {
//	创建一个学生对象
		Student s = new Student();
		s.setStuID(1);
		s.setStuName("Tom");
		
//	把这个对象保存到文件中 --> 序列化
		OutputStream out = new FileOutputStream("f:\\temp\\students.ooo");
		ObjectOutputStream oos = new ObjectOutputStream(out);
		
		oos.writeObject(s);
		
		oos.close();
		out.close();
	}
}
