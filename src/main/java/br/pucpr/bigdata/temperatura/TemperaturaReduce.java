package br.pucpr.bigdata.temperatura;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperaturaReduce extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(Text chave, Iterable<Text> valores, Context context)
            throws IOException, InterruptedException {

        double somaTotal = 0;
        long quantidadeTotal = 0;

        for (Text valor : valores) {
            String[] partes = valor.toString().split(";");
            somaTotal += Double.parseDouble(partes[0]);
            quantidadeTotal += Long.parseLong(partes[1]);
        }

        if (quantidadeTotal > 0) {
            double media = somaTotal / quantidadeTotal;
            context.write(new Text("media_temperatura"), new DoubleWritable(media));
        }
    }
}
