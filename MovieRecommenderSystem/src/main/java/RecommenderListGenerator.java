import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommenderListGenerator {
    public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        //filter out watched movie
        //match movie name to movie id.
        // map method
        Map<Integer, List<Integer>> watchHistory = new HashMap<>();
        protected void setup(Context context) throws IOException {
            //read movie watch list
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("watchHistory");
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            FileStatus fileStatus[] = fs.listStatus(pt);

            for (int i = 0; i < fileStatus.length; i++) {
                Path inFile = new Path(fileStatus[i].getPath().toString());
                String pthName = fileStatus[i].getPath().toString();
                String fileType = pthName.substring(pthName.length()-3,pthName.length());
                if (!fileType.toLowerCase().equals("txt")) {
                    continue;
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
                String line = br.readLine();

                while (line != null) {
                    int user = Integer.parseInt(line.split(",")[0]);
                    int movie = Integer.parseInt(line.split(",")[1]);
                    if (watchHistory.containsKey(user)) {
                        watchHistory.get(user).add(movie);
                    } else {
                        List<Integer> list = new ArrayList<>();
                        list.add(movie);
                        watchHistory.put(user, list);
                    }
                    line = br.readLine();
                }
                br.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //recommender   user \t movie:rating
            String[] tokens = value.toString().split("\t");
            int user = Integer.parseInt(tokens[0]);
            String movie_rating =  tokens[1];
            int movie = Integer.parseInt(movie_rating.split(":")[0]);
            if (!watchHistory.get(user).contains(movie)) {
                context.write(new IntWritable(user), new Text(movie_rating));
            }
        }
    }

    public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        // reduce method
        private Map<Integer, String> movieTitles = new HashMap<>();
        //match movie name to movie id
        protected void setup(Context context) throws IOException {
            //map movie id   movie title.
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("movieTitles");
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            FileStatus fileStatus[] = fs.listStatus(pt);

            for (int i = 0; i < fileStatus.length; i++) {
                Path inFile = new Path(fileStatus[i].getPath().toString());
                String pthName = fileStatus[i].getPath().toString();
                String fileType = pthName.substring(pthName.length() - 3, pthName.length());
                if (!fileType.toLowerCase().equals("txt")) {
                    continue;
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
                String line = br.readLine();
                while (line != null) {
                    int movie_id = Integer.parseInt(line.trim().split(",")[0]);
                    movieTitles.put(movie_id, line.trim().split(",")[1]);
                    line = br.readLine();
                }
                br.close();
            }
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //match movie name to movie id
            //key user , val movieidrating.
            while (values.iterator().hasNext()) {
                String cur = values.iterator().next().toString();
                int movie_id = Integer.parseInt(cur.split(":")[0]);
                String rating = cur.split(":")[1];
                context.write(key, new Text(movieTitles.get(movie_id) + "@" + rating));
            }




        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("watchHistory",args[0]);
        conf.set("movieTitles",args[1]);

        Job job = Job.getInstance(conf);
        job.setMapperClass(RecommenderListGeneratorMapper.class);
        job.setReducerClass(RecommenderListGeneratorReducer.class);

        job.setJarByClass(RecommenderListGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[2]));
        TextOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
    }

}
