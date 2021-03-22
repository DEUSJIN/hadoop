package pers.jin.mapreduce.partitioner1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/9 20:24
 */
public class Reducer1 extends Reducer<Text, Bean1, Text, Bean1> {
    private Bean1 outV = new Bean1();
    private long upSum;
    private long downSum;

    @Override
    protected void reduce(Text key, Iterable<Bean1> values, Context context) throws IOException, InterruptedException {
        upSum = 0;
        downSum = 0;
        for (Bean1 value : values) {
            upSum += value.getUpFlow();
            downSum += value.getDownFlow();
        }
        outV.setUpFlow(upSum);
        outV.setDownFlow(downSum);
        outV.setSumFlow();
        context.write(key, outV);
    }
}
