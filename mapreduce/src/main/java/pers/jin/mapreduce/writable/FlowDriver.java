package pers.jin.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/2 19:08
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);

        job.setReducerClass(FlowReducer.class);
        job.setMapperClass(FlowMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\LearnData\\hadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\LearnData\\hadoop\\output"));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
