package pers.jin.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/2 18:56
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");
        outK.set(strings[1]);
        outV.setUpFlow(Long.parseLong(strings[strings.length-3]));
        outV.setDownFlow(Long.parseLong(strings[strings.length-2]));
        outV.setSumFlow();
        context.write(outK, outV);
    }
}
