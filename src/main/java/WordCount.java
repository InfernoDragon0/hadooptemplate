import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.CsvIterator;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.topics.TopicInferencer;
import cc.mallet.types.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordCount {

    public static List<String> stopwords = new ArrayList<String>();
    public static List<String> dict = new ArrayList<String>();
    public static List<List<String>> documents = new ArrayList<List<String>>();
    public static List<String> docs = new ArrayList<String>();

    public static HashMap<Integer,List<String>> topics = new HashMap<Integer, List<String>>();

    public static double topicAlpha = 0.;

    public static void main(String[] args) throws Exception {

        if (args[0].equals("hadoopOnly")) {
            runHadoop(args[1], args[2]);
        }
        else if (args[0].equals("all")) {
            runHadoop(args[1], args[2]);
            runMallet();
        }
        else if (args[0].equals("malletOnly")) {
            runMallet();
        }
    }

    public static void runHadoop(String input, String output) throws Exception {
        BasicConfigurator.configure();
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
        ChainMapper.addMapper(job, WordCountMapper.class,Text.class, LongWritable.class, Text.class, LongWritable.class, swConf);

        job.setMapperClass(ChainMapper.class);
        job.setCombinerClass(WordExtractorReducer.class);
        job.setReducerClass(WordExtractorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
        System.out.println("Now writing cleansed data into output");
        File myObj = new File("output/cleansed.txt");
        if (myObj.createNewFile()) {
            PrintWriter writer = new PrintWriter("output/cleansed.txt", "UTF-8");
            for (String x : docs) {
                writer.println(x);
            }
            writer.close();
        }

    }

    public static void runMallet() throws Exception {
        // Begin by importing documents from text to feature sequences
        ArrayList<Pipe> pipeList = new ArrayList<Pipe>();

        pipeList.add( new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) );
        pipeList.add( new TokenSequence2FeatureSequence() );

        InstanceList instances = new InstanceList (new SerialPipes(pipeList));

        Reader fileReader = new InputStreamReader(new FileInputStream("output/cleansed.txt"), "UTF-8");
        instances.addThruPipe(new CsvIterator(fileReader, Pattern.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"),
                3, 2, 1)); // data, label, name fields

        int numTopics = 30;
        ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);

        model.addInstances(instances);

        // Use two parallel samplers, which each look at one half the corpus and combine
        //  statistics after every iteration.
        model.setNumThreads(16);
        model.setNumIterations(1000);
        model.estimate();


        // The data alphabet maps word IDs to strings
        Alphabet dataAlphabet = instances.getDataAlphabet();

        FeatureSequence tokens = (FeatureSequence) model.getData().get(0).instance.getData();
        LabelSequence topics = model.getData().get(0).topicSequence;

        Formatter out = new Formatter(new StringBuilder(), Locale.US);
        for (int position = 0; position < tokens.getLength(); position++) {
            out.format("%s-%d ", dataAlphabet.lookupObject(tokens.getIndexAtPosition(position)), topics.getIndexAtPosition(position));
        }
        System.out.println(out);

        // Estimate the topic distribution of the first instance,
        //  given the current Gibbs state.
        double[] topicDistribution = model.getTopicProbabilities(0);

        // Get an array of sorted sets of word ID/count pairs
        ArrayList<TreeSet<IDSorter>> topicSortedWords = model.getSortedWords();

        File myObj = new File("output/topicModel.txt");
        PrintWriter writer = new PrintWriter("output/topicModel.txt", "UTF-8");

        // Show top 5 words in topics with proportions for the first document
        for (int topic = 0; topic < numTopics; topic++) {
            Iterator<IDSorter> iterator = topicSortedWords.get(topic).iterator();

            out = new Formatter(new StringBuilder(), Locale.US);
            out.format("%d\t%.3f\t", topic, topicDistribution[topic]);
            int rank = 0;
            while (iterator.hasNext() && rank < 5) {
                IDSorter idCountPair = iterator.next();
                out.format("%s (%.0f) ", dataAlphabet.lookupObject(idCountPair.getID()), idCountPair.getWeight());
                rank++;
            }
            writer.println("Topic " + out);
            System.out.println("Topic " + out);
        }
        writer.close();
        // Create a new instance with high probability of topic 0
        StringBuilder topicZeroText = new StringBuilder();
        Iterator<IDSorter> iterator = topicSortedWords.get(0).iterator();

        int rank = 0;
        while (iterator.hasNext() && rank < 5) {
            IDSorter idCountPair = iterator.next();
            topicZeroText.append(dataAlphabet.lookupObject(idCountPair.getID()) + " ");
            rank++;
        }

        // Create a new instance named "test instance" with empty target and source fields.
        InstanceList testing = new InstanceList(instances.getPipe());
        testing.addThruPipe(new Instance(topicZeroText.toString(), null, "test instance", null));

        TopicInferencer inferencer = model.getInferencer();
        double[] testProbabilities = inferencer.getSampledDistribution(testing.get(0), 10, 1, 5);
        System.out.println("get topic 0 distribution = " + topicZeroText.toString() + "\t" + testProbabilities[0]);
    }
}
