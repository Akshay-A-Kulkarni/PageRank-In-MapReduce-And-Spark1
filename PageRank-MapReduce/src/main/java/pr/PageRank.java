package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class
PageRank extends Configured implements Tool {

    private static Double bigNum = Math.pow(10D,15D);

    public enum COUNTER {
        SinkMass,
        GraphSize
    };

    private static final Logger logger = LogManager.getLogger(PageRank.class);

    public static class RankerMapper extends Mapper<Object, Text, IntWritable, Vertex> {


        @Override
        public void map(final Object key, final Text input, final Context context) throws IOException, InterruptedException {
            final IntWritable k = new IntWritable();

            final Vertex v = new Vertex();

            v.deserializeFromString(input.toString());

            context.write(new IntWritable(v.getVertex()),v);

            Double sink_dist =  ((v.getPR()/v.getAdjacencyList().size())*bigNum);
            Double pr_dist  = v.getPR()/v.getAdjacencySize();

            for (Integer i: v.getAdjacencyList()) {
                if (i.equals(0)){
                    context.getCounter(COUNTER.SinkMass).increment(sink_dist.longValue());
                }
                else {
                    Vertex node = new Vertex();
                    node.setVertex(i);
                    k.set(i);
                    node.setPR(pr_dist);
                    node.setflag(false);
                    context.write(k,node);
                }
            }
        }
    }

    public static class RankerReducer extends Reducer<IntWritable, Vertex, Text, Text> {
        private long countval;
        private Vertex V = null;



        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();

            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            countval = currentJob.getCounters().findCounter(COUNTER.SinkMass).getValue();
//            JobClient jc = new JobClient(conf);
//            jc.getJob(context.getJobID());
//            JobClient.getJob(conf).getcounters();
        }

        @Override
        public void reduce(final IntWritable key, final Iterable<Vertex> values, final Context context)
                throws IOException, InterruptedException {
            Double accumulated = 0.0 ;
            ArrayList test = new ArrayList();

            for (final Vertex val : values) {
                if(val.fullVertexFlag) {
                     V = val;
                }
                else {
                    test.add(val.getPR());
                    accumulated += val.getPR();
                }
            }

            Double sink_dist = (countval/1000*1000*bigNum);

            logger.error(sink_dist.toString());

            Double new_pr = 0.15*(0.25) + 0.85*((sink_dist)+accumulated);

            V.setPR(new_pr);

            context.write(new Text(V.serializeToString()), new Text(""));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        for (int iter = 0; iter < Integer.parseInt(args[2])+1;iter++) {
            Configuration conf = getConf();
            Job job = Job.getInstance(conf, "Rank Count");
            job.setJarByClass(PageRank.class);
            Configuration jobConf = job.getConfiguration();
//          jobConf.set("mapreduce.output.textoutputformat.separator", "");
//		     Delete output directory, only to ease local development; will not work on AWS. ===========
//            final FileSystem fileSystem = FileSystem.get(conf);
//            if (fileSystem.exists(new Path(args[1]))) {
//                fileSystem.delete(new Path(args[1]), true);
//            }
//		     ================
            String path = args[1];
            job.setMapperClass(RankerMapper.class);
            job.setReducerClass(RankerReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1000);
            if (iter == 0) {
                NLineInputFormat.addInputPath(job, new Path(args[0]));
            }
            else{
                NLineInputFormat.addInputPath(job, new Path(path+"/"+iter));
            }

            FileOutputFormat.setOutputPath(job, new Path(path+"/"+(iter+1)));

            job.waitForCompletion(true);

        }
        return 1;

    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir> <num-iters>");
        }
        try {
            ToolRunner.run(new PageRank(), args);
        } catch (final Exception e) {
            logger.error("", e);

        }
    }

}