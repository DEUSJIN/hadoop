package pers.jin.mapreduce.partitioner1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/9 21:07
 */
public class MyPartitioner extends Partitioner<Text,Bean1> {
    @Override
    public int getPartition(Text text, Bean1 bean1, int numPartitions) {
        String prePhone = text.toString().substring(0, 3);
        int partition = 4;
        if("136".equals(prePhone)){
            partition = 0;
        }else if ("137".equals(prePhone)){
            partition = 1;
        }else if ("138".equals(prePhone)){
            partition = 2;
        }else if ("139".equals(prePhone)){
            partition = 3;
        }
        return partition;
    }
}
