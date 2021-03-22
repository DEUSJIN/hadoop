package pers.jin.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/16 19:48
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private Text outK = new Text();
    private TableBean outV = new TableBean();
    private String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if ("order.txt".equals(name)) {
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setPname("");
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setFlag("order");
        } else {
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setPname(split[1]);
            outV.setAmount(0);
            outV.setFlag("pd");
        }
        context.write(outK, outV);
    }
}
