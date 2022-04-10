import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static List<String> stopwords = new ArrayList<String>();
    public static List<String> dict = new ArrayList<String>();
    public static List<List<String>> documents = new ArrayList<List<String>>();

    public static HashMap<Integer,List<String>> topics = new HashMap<Integer, List<String>>();

    public static double topicAlpha = 0.;

    public static void main(String[] args) throws Exception {
        //BasicConfigurator.configure();
        System.out.println("Loading stopwords");
        File f = new File("configs/stopwords.txt");
        BufferedReader br = new BufferedReader(new FileReader(f));
        String sw;
        while ((sw = br.readLine()) != null) {
            stopwords.add(sw);
        }

        System.out.println("Loading Dictionary");
        File f2 = new File("configs/words_alpha.txt");
        BufferedReader br2 = new BufferedReader(new FileReader(f2));
        String sw2;
        while ((sw2 = br2.readLine()) != null) {
            dict.add(sw2);
        }

        System.out.println("Loaded " + dict.size() + " dictionary words");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        //first cleanse the words and put them in the bag of words
        Configuration weConf = new Configuration(false);
        Configuration swConf = new Configuration(false);
        ChainMapper.addMapper(job, WordExtractorMapper.class, LongWritable.class, Text.class, Text.class, LongWritable.class, weConf);
        //ChainMapper.addMapper(job, TopicAssignerMapper.class,Text.class, LongWritable.class, Text.class, LongWritable.class, swConf);



        job.setMapperClass(ChainMapper.class);
        job.setCombinerClass(WordExtractorReducer.class);
        job.setReducerClass(WordExtractorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        for (List<String> doc : documents)
            System.out.println(Arrays.toString(doc.toArray()));




        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
