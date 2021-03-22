package pers.jin.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/22 18:46
 */
public class TopNDriver {
    private static Tool tool;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        switch (args[0]) {
            case "topn":
                tool = new TopN();
                break;
            default:
                throw new RuntimeException("no such tool " + args[0]);
        }
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}
