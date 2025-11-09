package addRegion;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class AddRegionJob extends Configured implements Tool {

    public static class AddRegionMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
        public Text outputValue = new Text();
        public NullWritable outputKey = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = StringUtils.split(value.toString(), '\\', ',');
            String state = words[1].trim().toLowerCase();
            if ((int)state.charAt(0) <= (int)'a' + 12){
                outputValue.set(value.toString() + ",Southern");
            }else{
                outputValue.set(value.toString() + ",Northern");
            }
            context.write(outputKey, outputValue);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "AddRegionJob");
        job.setJarByClass(AddRegionJob.class);

        Path out = new Path("CountiesWRegion");
        out.getFileSystem(conf).delete(out, true);
        FileInputFormat.setInputPaths(job, "counties");
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(AddRegionMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) {
        int result = 0;
        try{
            result = ToolRunner.run(new Configuration(), new AddRegionJob(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(result);
    }
}
