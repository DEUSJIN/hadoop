package pers.jin.mapreduce.partitioner2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/10 19:24
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        outV.set(split[1]);
        outK.setUpFlow(Long.parseLong(split[split.length - 3]));
        outK.setDownFlow(Long.parseLong(split[split.length - 2]));
        outK.setSumFlow();
        context.write(outK, outV);
    }
}
