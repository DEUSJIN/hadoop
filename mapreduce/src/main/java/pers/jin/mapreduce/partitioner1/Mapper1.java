package pers.jin.mapreduce.partitioner1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/9 20:19
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, Bean1> {
    private Text outK = new Text();
    private Bean1 outV = new Bean1();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        outK.set(split[1]);
        outV.setUpFlow(Long.parseLong(split[split.length-3]));
        outV.setDownFlow(Long.parseLong(split[split.length-2]));
        outV.setSumFlow();
        context.write(outK, outV);
    }
}
