package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataLoad {

	public static void main(String[] args) {

		System.out.println("Started ..");

		try {

				System.out.println("inside main if..");
				copyFileToHdfs();
			

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("main over ..");

	}

	private static void copyFileToHdfs() throws IOException {

		System.out.println("inside copyFileToHdfs");

		//Path localSrc = new Path("/home/cloudera/shared/table/example.txt");
		Path localSrc = new Path("/home/cloudera/hdp/pigandhive/labs/MR_Workflow/Project1/output/example.txt");
		Path dir = new Path("table6");

		Configuration conf = new Configuration();

		System.out.println("Conf  : " + conf);

		FileSystem fs = FileSystem.get(conf);

		System.out.println("fs  :" + fs.getConf());

		if (!fs.exists(dir)) {
			System.out.println("inside copy if..");
			fs.mkdirs(dir);
			System.out.println("Created table directory on HDFS");
		} else {
			// fs.delete(dir, true);
			System.out.println("inside copy else..");
		}

		Path dest1 = new Path("table6/" + "table.txt");

		fs.copyFromLocalFile(localSrc, dest1);

		fs.close();

		System.out.println("Copy done ..");

	}

}
