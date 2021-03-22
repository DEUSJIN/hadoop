package pers.jin.mapreduce.partitioner1;

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
 * @Date: 2021/3/9 20:27
 */
public class Driver1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Driver1.class);

        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Bean1.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\LearnData\\hadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\LearnData\\hadoop\\output"));

//        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
