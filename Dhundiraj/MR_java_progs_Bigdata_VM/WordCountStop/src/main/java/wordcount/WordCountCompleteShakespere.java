package wordcount;

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
import org.apache.log4j.Logger;

import java.awt.print.Book;
import java.io.*;
import java.util.HashSet;
import java.util.Scanner;


/**
 * @author Ravinshu Makkar (PRN : 44)
 * @version 1.0
 * <p>
 * WordCountCompleteShakespere is the main class for a Hadoop MapReduce application that counts
 * word frequencies in an input file, excluding a predefined list of stop words.
 * It implements the Tool interface to allow configuration-driven execution.
 */
public class WordCountCompleteShakespere extends Configured implements Tool {


    /**
     * StopWordMapper is the Mapper for the WordCount job.
     * It reads input lines, tokenizes them into words, filters out stop words,
     * cleans the remaining words, and emits (word, 1) pairs.
     * The input key is the byte offset (LongWritable), and the value is the line (Text).
     * The output is (Text, IntWritable).
     */
    public static class StopWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private HashSet<String> stopWordSet = new HashSet<String>();
        protected Text outputKey = new Text();
        protected IntWritable outputValue = new IntWritable();
        Logger logger = Logger.getLogger(StopWordMapper.class);
    
        /**
         * to load the stop words from a local file into a HashSet.
         *
         * @param context The context object
         */
        @Override
        protected void setup(Context context) {
            String all_stop_words = "i,me,my,myself,we,our,ours,ourselves,you,your,yours,yourself,yourselves,he,him,his,himself,she,her,hers,herself,it,its,itself,they,them,their,theirs,themselves,what,which,who,whom,this,that,these,those,am,is,are,was,were,be,been,being,have,has,had,having,do,does,did,doing,a,an,the,and,but,if,or,because,as,until,while,of,at,by,for,with,about,against,between,into,through,during,before,after,above,below,to,from,up,down,in,out,on,off,over,under,again,further,then,once,here,there,when,where,why,how,all,any,both,each,few,more,most,other,some,such,nonor,not,only,own,same,so,than,too,very,can,will,just,don't,should,now";
            try {
                String[] dataSplit = StringUtils.split(all_stop_words, '\\', ',');
                for (String word : dataSplit) {
                    // trim and to lowercase to match exact word and manage if duplicated
                    stopWordSet.add(word.trim().toLowerCase());
                }
            } catch (Exception e) {
                logger.info("Exited because file not found");
                System.exit(15888);
            }
        }


        /**
         * The map method processes a single input record (a line of text).
         * It tokenizes the line, checks if each word is a stop word, cleans the word,
         * and emits (word, 1) if it is not a stop word.
         *
         * @param key     The input key
         * @param value   The input value (the line of text).
         * @param context The context object to emit the output key/value pairs.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = StringUtils.split(value.toString(), '\\', ' ');
            for (String word : words) {
                logger.info("Current word: |" + word + "|");
                if (!stopWordSet.contains(word.replaceAll("[^a-zA-Z]", "").trim().toLowerCase())) {
                    logger.info("Selected word: |" + word.replaceAll("[^a-zA-Z]", "").trim().toLowerCase() + "|");
                    word = word.replaceAll("[^a-zA-Z]", "").trim();
                    outputKey.set(word);
                    outputValue.set(1);
                    context.write(outputKey, outputValue);
                } else {
                    logger.info("Not Selected word: |" + word.replaceAll("[^a-zA-Z]", "").trim().toLowerCase() + "|");
                }
            }
        }
    }


    /**
     * The input/output key is the word (Text), and the value is the count (IntWritable).
     */
    public static class StopWordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected Text outputKey = new Text();
        protected IntWritable outputValue = new IntWritable();


        /**
         * to sum the counts for identical keys (words)
         *
         * @param key     The input key (a word).
         * @param value   An iterable of counts (IntWritable) for that word.
         * @param context The context object to emit the reduced key/value pair.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            while (value.iterator().hasNext()) {
                sum += value.iterator().next().get();
            }
            outputKey.set(key);
            outputValue.set(sum);
            context.write(outputKey, outputValue);
        }
    }


    /**
     * It receives all counts for a single word and calculates the final total frequency.
     * The input/output key is the word (Text), and the value is the final total count (IntWritable).
     */
    public static class StopWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected Text outputKey = new Text();
        protected IntWritable outputValue = new IntWritable();


        /**
         * It sums all the individual counts (from mappers and combiners) for that key.
         *
         * @param key     The input key (a word).
         * @param value   An iterable of counts (IntWritable) for that word.
         * @param context The context object to emit the final total count.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            while (value.iterator().hasNext()) {
                sum += value.iterator().next().get();
            }
            outputKey.set(key);
            outputValue.set(sum);
            context.write(outputKey, outputValue);
        }
    }

    /**
     * @param args args[0] is the input path, args[1] is the output path.
     * @return 0 if the job succeeds, 1 otherwise.
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "WordCountCompleteShakespere");
        job.setJarByClass(WordCountCompleteShakespere.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        out.getFileSystem(conf).delete(out, true);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setNumReduceTasks(3);
        job.setMapperClass(StopWordMapper.class);
        job.setReducerClass(StopWordReducer.class);
        job.setCombinerClass(StopWordCombiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(false) ? 0 : 1;
    }

    /**
     * @param args Command line arguments (input path, output path).
     */
    public static void main(String[] args) {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(), new WordCountCompleteShakespere(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }
}