package pers.jin.mapreduce.TopN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/19 16:54
 */
public class TopN {
    private static Text mapOutV = new Text();

    private static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            mapOutV.set(value);
            context.write(NullWritable.get(), mapOutV);
        }
    }

    private static Text reduceOutV = new Text();
    private static class MyReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) {

    }
}
