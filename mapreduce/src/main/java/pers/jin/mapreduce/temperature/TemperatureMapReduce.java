package pers.jin.mapreduce.temperature;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/19 16:10
 */
public class TemperatureMapReduce {
    private static IntWritable mapOutV = new IntWritable();
    private static IntWritable mapOutK = new IntWritable();

    private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            System.out.println(Arrays.toString(split));
            mapOutK.set(Integer.parseInt(split[0] + split[1]));
            mapOutV.set(Integer.parseInt(split[3]));
            context.write(mapOutK, mapOutV);
        }
    }

    private static Text reduceOutV = new Text();

    private static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> integers = new ArrayList<>();
            for (IntWritable value : values) {
                integers.add(value.get());
            }
            integers.sort((o1, o2) -> o2 - o1);
            String s = integers.toString();
            s = s.substring(1, s.length() - 1);
            reduceOutV.set(s);
            context.write(key, reduceOutV);
        }
    }

    private static class MyIntWritable extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TemperatureMapReduce.class);

        job.setMapperClass(TemperatureMapReduce.MyMapper.class);
        job.setReducerClass(TemperatureMapReduce.MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\LearnData\\hadoop\\inputtemp"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\LearnData\\hadoop\\output"));

        job.setSortComparatorClass(MyIntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
