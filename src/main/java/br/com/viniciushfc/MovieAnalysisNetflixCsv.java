package br.com.viniciushfc;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MovieAnalysisNetflixCsv {

    private static final Set<String> STOPWORDS = new HashSet<>();

    static {
        String[] words = {"de", "em", "para", "a", "o", "e", "do", "da", "dos", "das", "um", "uma", "no", "na", "nos", "nas", "com", "por", "que"};
        for (String w : words) STOPWORDS.add(w);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (files.length < 2) {
            System.err.println("Usage: MovieAnalysisNetflixCsv <input> <output>");
            System.exit(-1);
        }

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "movie-analysis-netflix-csv");
        job.setJarByClass(MovieAnalysisNetflixCsv.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split(",", 2);
            if (parts.length < 2) return;

            String title = parts[0].trim();
            String desc = parts[1].toLowerCase().replaceAll("[^a-z0-9\\s]", " ").trim();
            StringTokenizer tokenizer = new StringTokenizer(desc);
            int wordCount = 0;

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (!STOPWORDS.contains(word) && !word.isEmpty()) {
                    outKey.set(word);
                    context.write(outKey, one);
                    wordCount++;
                }
            }

            outKey.set("TITLE:" + title);
            context.write(outKey, new IntWritable(wordCount));
        }
    }

    public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, String> topWordsMax = new TreeMap<>();
        private TreeMap<Integer, String> topWordsMin = new TreeMap<>();

        private int totalWords = 0;
        private int totalTitles = 0;
        private String titleMax = "";
        private int maxWords = 0;
        private String titleMin = "";
        private int minWords = Integer.MAX_VALUE;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            String keyStr = key.toString();

            if (keyStr.startsWith("TITLE:")) {
                String title = keyStr.substring(6);
                totalWords += sum;
                totalTitles++;
                if (sum > maxWords) {
                    maxWords = sum;
                    titleMax = title;
                }
                if (sum < minWords) {
                    minWords = sum;
                    titleMin = title;
                }
            } else {
                totalWords += sum;

                topWordsMax.put(sum, keyStr);
                if (topWordsMax.size() > 5) topWordsMax.remove(topWordsMax.firstKey());

                topWordsMin.put(sum, keyStr);
                if (topWordsMin.size() > 5) topWordsMin.remove(topWordsMin.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Título com mais palavras: " + titleMax), new IntWritable(maxWords));
            context.write(new Text("Título com menos palavras: " + titleMin), new IntWritable(minWords));
            context.write(new Text("Número total de palavras:"), new IntWritable(totalWords));
            context.write(new Text("Média de palavras por descrição:"), new IntWritable(totalWords / totalTitles));

            context.write(new Text("Top 5 palavras mais frequentes:"), null);
            for (Map.Entry<Integer, String> entry : topWordsMax.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }

            context.write(new Text("Top 5 palavras menos frequentes:"), null);
            for (Map.Entry<Integer, String> entry : topWordsMin.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }
}
