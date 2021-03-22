package pers.jin.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/10 19:30
 */
public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int partition = 4;
        String s = text.toString().substring(0, 3);
        System.out.println(s);
        if("136".equals(s)){
            partition = 0;
        }else if("137".equals(s)){
            partition = 1;
        }else if("138".equals(s)){
            partition = 2;
        }else if("139".equals(s)){
            partition = 3;
        }
        return partition;
    }
}
