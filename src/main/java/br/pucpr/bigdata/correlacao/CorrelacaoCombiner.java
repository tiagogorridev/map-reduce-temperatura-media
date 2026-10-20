package br.pucpr.bigdata.correlacao;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// soma os valores dentro de cada maquina antes de enviar ao Reduce (nao calcula a correlacao aqui)
public class CorrelacaoCombiner extends Reducer<Text, PearsonWritable, Text, PearsonWritable> {

    @Override
    protected void reduce(Text chave, Iterable<PearsonWritable> valores, Context context)
            throws IOException, InterruptedException {

        PearsonWritable parcial = new PearsonWritable();
        for (PearsonWritable valor : valores) {
            parcial.somar(valor);
        }

        context.write(chave, parcial);
    }
}
