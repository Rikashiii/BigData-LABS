package freq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FreqJob extends Configured implements Tool {

    public static class FreqMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        public Text outputKey = new Text();
        public IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] sep = StringUtils.split(value.toString(), '\\', '\t');
            String fName = sep[0];
            String lName = sep[1];

            outputKey.set(fName + ' ' + lName);
            outputValue.set(1);

            context.write(outputKey, outputValue);

        }

    }

    public static class FreqCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        public IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            while (value.iterator().hasNext()){
                sum++;
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    public static class FreqReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            while (value.iterator().hasNext()){
                sum+=value.iterator().next().get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "VisitsFrequencyJob");
        job.setJarByClass(FreqJob.class);

        Path out = new Path("VisitFreqOutput");
        Path in = new Path("/user/hive/warehouse/wh_visits");
        out.getFileSystem(conf).delete(out, true);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(FreqMapper.class);
        job.setReducerClass(FreqCombiner.class);
        job.setReducerClass(FreqReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new FreqJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }

}
