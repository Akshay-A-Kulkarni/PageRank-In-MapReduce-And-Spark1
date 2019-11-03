package pr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRank.class);

    public static class RankerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text vertex = new Text();
        private final Text mass = new Text();


        @Override
        public void map(final Object key, final Text input, final Context context) throws IOException, InterruptedException {
            final String[] values  = input.toString().replaceAll("[#]", "").split("@");
            final Text v = new Text(values[0]);
            double pr = Double.parseDouble(values[1]);

            Double dist = 0.0;

            final String[] adj = values[2].replaceAll("[\\[\\]]","").split(",");

            final Double m = pr/adj.length;
            for (String s:adj) {
                if (s.equals("0"))
                    dist = 0.5;
                else {
                    vertex.set(s);
                    mass.set(m.toString());
                    context.write(vertex, mass);
                }
            }

            context.write(v,input);
        }
    }

    public static class RankerReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Double accumulated = 0.0 ;
            Vertex v = new Vertex(Integer.parseInt(key.toString()));
            for (final Text val : values) {
                if(val.charAt(0) == '#') {
                    v.deserializeFromString(val.toString());
                }
                else {
                    accumulated += Double.parseDouble(val.toString());
                }
            }
            Double new_pr = 0.15*(0.25) + 0.85*((0.125)+accumulated);
            context.write(key, new Text(new_pr.toString()));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Rank Count");
        job.setJarByClass(PageRank.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ":");
//		 Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
//		 ================
        job.setMapperClass(RankerMapper.class);
        job.setReducerClass(RankerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new PageRank(), args);
        } catch (final Exception e) {
            logger.error("", e);

        }
    }

}