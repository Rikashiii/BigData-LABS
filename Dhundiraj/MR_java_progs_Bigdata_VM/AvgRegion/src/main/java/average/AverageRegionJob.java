package average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AverageRegionJob extends Configured implements Tool {

    public static class AvgRegionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public Text outputKey = new Text();
        public IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = StringUtils.split(value.toString(), '\\', ',');
            int income = 0;
            try {
                income = Integer.parseInt(words[9].trim());
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
            outputKey.set(words[words.length - 1].trim());
            outputValue.set(income);
            context.write(outputKey, outputValue);
        }
    }

    public static class AvgRegionReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public Text outputKey = new Text();
        public DoubleWritable outputValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int counter = 0;
            double sum = 0;
            while (value.iterator().hasNext()) {
                counter++;
                sum += value.iterator().next().get();
            }
            outputKey.set(key);
            try {

                outputValue.set(sum / counter);

            } catch (ArithmeticException e) {
                e.printStackTrace();
            }
            context.write(outputKey, outputValue);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "AverageRegionJob");
        job.setJarByClass(AverageRegionJob.class);

        Path out = new Path("avgRegion");
        out.getFileSystem(conf).delete(out, true);

        FileInputFormat.setInputPaths(job, "CountiesWRegion");
        FileOutputFormat.setOutputPath(job, out);


        job.setReducerClass(AvgRegionReducer.class);
        job.setMapperClass(AvgRegionMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new AverageRegionJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }

}
