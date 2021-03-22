package pers.jin.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/2 15:54
 */
public class WordCountReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable,Text,IntWritable> {
    private int sum;
    private IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
