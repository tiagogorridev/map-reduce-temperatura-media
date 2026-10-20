package br.pucpr.bigdata.temperatura;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// soma as temperaturas e a quantidade dentro de cada maquina após o map
public class TemperaturaCombiner extends Reducer<Text, SomaContagemWritable, Text, SomaContagemWritable> {

    @Override
    protected void reduce(Text chave, Iterable<SomaContagemWritable> valores, Context context)
            throws IOException, InterruptedException {

        double somaParcial = 0;
        long quantidadeParcial = 0;

        for (SomaContagemWritable valor : valores) {
            somaParcial += valor.getSoma();
            quantidadeParcial += valor.getQuantidade();
        }

        // envio do resumo (nao divide aqui para nao virar media das medias)
        context.write(chave, new SomaContagemWritable(somaParcial, quantidadeParcial));
    }
}
