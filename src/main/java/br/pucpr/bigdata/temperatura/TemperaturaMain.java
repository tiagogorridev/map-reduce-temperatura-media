package br.pucpr.bigdata.temperatura;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// configura e roda o calculo da temperatura media do arquivo
public class TemperaturaMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: TemperaturaMain <entrada> <saida>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // criacao do job
        Job job = Job.getInstance(conf, "media-temperatura");
        job.setJarByClass(TemperaturaMain.class);

        // diz quem é o Map, o Reduce e liga o Combiner (menos trafego de dados)
        job.setMapperClass(TemperaturaMap.class);
        job.setReducerClass(TemperaturaReduce.class);
        job.setCombinerClass(TemperaturaCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SomaContagemWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // garante media geral
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}