import java.util.*;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountCat extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountCat(), args);
        System.exit(res);
    }

    @Override
    public int run(String args[])throws Exception {
        Configuration conf= getConf();
        conf.set("mapred.job.queue.name","d_bi");
        String inputPath = args[0];
        String outputPath = args[1];
        cleanHDFSOutPath(new Path(outputPath));

        Pipeline pipeline = new MRPipeline(CountCat.class, getConf());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> words = lines.parallelDo(new GetWords(), Writables.strings());

        PTable<String, Long> counts = words.count();

        pipeline.writeTextFile(counts, outputPath);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }
    private void cleanHDFSOutPath(Path pathToDelete) {
        try {
            Configuration conf = getConf();
            FileSystem hdfs = FileSystem.get(URI.create(pathToDelete.toString()),conf);
            //g_activityLogger.info("Cleaning OutPut Path: " + pathToDelete.toString());
            if (hdfs.exists(pathToDelete)) {
                hdfs.delete(pathToDelete, true);
            }
        } catch (Exception exp) {
            exp.fillInStackTrace();
        }
    }
}