package pers.jin.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/22 18:36
 */
public class TopN implements Tool {
    private static int n = 5;
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(TopNDriver.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setSortComparatorClass(MyIntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    public static class MyIntWritable extends IntWritable.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    public static class TopNMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static IntWritable outK = new IntWritable();
        private static Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            outK.set(Integer.parseInt(split[1]));
            outV.set(split[0]);
            context.write(outK, outV);
        }
    }

    public static class TopNReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (--n < 0) {
                    return;
                }
                context.write(key, value);
            }
        }
    }
}
