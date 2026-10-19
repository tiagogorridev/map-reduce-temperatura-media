package br.pucpr.bigdata.correlacao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Classe principal: configura e roda o calculo de correlacao entre a temperatura e as outras colunas
public class CorrelacaoMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: CorrelacaoMain <entrada> <saida>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "correlacao-temperatura");
        job.setJarByClass(CorrelacaoMain.class);

        job.setMapperClass(CorrelacaoMap.class);
        job.setReducerClass(CorrelacaoReduce.class);

        // combiner soma os valores em cada maquina antes de enviar, assim trafega menos dado pela rede
        job.setCombinerClass(CorrelacaoCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PearsonWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
