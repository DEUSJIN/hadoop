package pers.jin.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/2 15:54
 */
public class WordCountMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,Text, IntWritable> {
    private Text keyOut = new Text();
    private IntWritable valueOut = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(" ");

        for (String s : strs) {
            keyOut.set(s);
            context.write(keyOut,valueOut);
        }
    }
}
