import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Date;

public class StatisticsAnalysis {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StatisticsAnalysis");

        job.setJarByClass(StatisticsAnalysis.class);
        job.setMapperClass(StatisticsMapper.class);
        job.setCombinerClass(StatisticsReducer.class);
        job.setReducerClass(StatisticsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path inputPath = new Path("input");
        Path outputPath = new Path("output" + new Date().getTime());

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit((job.waitForCompletion(true))?0:1);

    }
}
