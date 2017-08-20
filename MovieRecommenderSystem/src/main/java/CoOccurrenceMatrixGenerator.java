import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoOccurrenceMatrixGenerator {
    public static class MartixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] user_movieRatings = line.split("\t");
            String user = user_movieRatings[0];
            String[] movieRatings = user_movieRatings[1].split(",");
            for (String movieRating : movieRatings) {
                String movie = movieRating.split(":")[0];
                for (String movieRating2 : movieRatings) {
                    String movie2 = movieRating2.split(":")[0];
                    context.write(new Text(movie + ":" + movie2), new IntWritable(1));
                }
            }



        }
    }

    public static class MartixGeneratorReducer extends Reducer< Text,IntWritable, Text,IntWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            while (values.iterator().hasNext()) {
                sum += values.iterator().next().get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(MartixGeneratorMapper.class);
        job.setReducerClass(MartixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
