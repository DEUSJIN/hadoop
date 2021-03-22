package pers.jin.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/2 19:04
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private long upSum;
    private long downSum;
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        upSum = 0;
        downSum = 0;
        for (FlowBean value : values) {
            upSum += value.getUpFlow();
            downSum += value.getDownFlow();
        }
        outV.setUpFlow(upSum);
        outV.setDownFlow(downSum);
        outV.setSumFlow();
        context.write(key, outV);
    }
}
