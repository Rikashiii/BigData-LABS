package wordcount;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountJob extends Configured implements Tool {
	
	public enum Counter{MAP, REDUCE};
	
	public static int partCounter = 0;
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable outputValue = new IntWritable();
		private Logger wordCountReduceLogger = Logger.getLogger(WordCountMapper.class);

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			context.getCounter(Counter.REDUCE).increment(1);
			int sum = 0;
			for(IntWritable count : values) {
				sum += count.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
		
		@Override
		protected void setup(Context context){
			wordCountReduceLogger.info("In setup for mapper called");
		}
		
		@Override
		protected void cleanup(Context context){
			wordCountReduceLogger.debug("In cleanup for mapper called");
		}

		

	}

	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private Text outputKey = new Text();
		private Logger wordCountMapLogger = Logger.getLogger(WordCountMapper.class);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String currentLine = value.toString();
			context.getCounter(Counter.MAP).increment(1);
			System.out.println("in map: " + context.getCounter(Counter.MAP).getValue());

			String [] words = StringUtils.split(currentLine, ' ');
			for(String word : words) {
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
		
		@Override
		protected void setup(Context context){
			wordCountMapLogger.info("In setup for mapper called");
		}
		
		@Override
		protected void cleanup(Context context){
			wordCountMapLogger.debug("In cleanup for mapper called");
		}

	}
	
	/*

	public static class WordCountPartitioner extends Partitioner<Text, IntWritable>{
		public int getPartition(Text key, IntWritable value, int numReduceTasks){
			System.out.println("Inside Partitioner: " + (++partCounter));

			if (numReduceTasks == 1) return 0;
			return (key.toString().length() * value.get()) % numReduceTasks;
		}
	}

	*/
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		Path in = new Path("wordcount_source");
		Path out = new Path("wordcount_result");
		out.getFileSystem(conf).delete(out, true); 
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
//		job.setPartitionerClass(WordCountPartitioner.class);
//		job.setNumReduceTasks(2);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), 
							new WordCountJob(),
							args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);
	}

}
