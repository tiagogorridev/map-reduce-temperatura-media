package br.pucpr.bigdata.temperatura;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// junta as somas de todas as maquinas e calcula a media final, uma vez no final após o shuffle
public class TemperaturaReduce extends Reducer<Text, SomaContagemWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text chave, Iterable<SomaContagemWritable> valores, Context context)
            throws IOException, InterruptedException {

        double somaTotal = 0;
        long quantidadeTotal = 0;

        for (SomaContagemWritable valor : valores) {
            somaTotal += valor.getSoma();
            quantidadeTotal += valor.getQuantidade();
        }

        if (quantidadeTotal > 0) {
            // divide so uma vez e envia a média
            double media = somaTotal / quantidadeTotal;
            context.write(new Text("media_temperatura"), new DoubleWritable(media));
        }
    }
}
