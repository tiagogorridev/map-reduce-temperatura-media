package br.pucpr.bigdata.correlacao;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Junta as somas de todas as maquinas e calcula a correlacao de cada coluna com a temperatura
public class CorrelacaoReduce extends Reducer<Text, PearsonWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text chave, Iterable<PearsonWritable> valores, Context context)
            throws IOException, InterruptedException {

        PearsonWritable total = new PearsonWritable();
        for (PearsonWritable valor : valores) {
            total.somar(valor);
        }

        double r = total.correlacao();
        if (total.getN() > 1 && !Double.isNaN(r)) {
            context.write(chave, new DoubleWritable(r));
        }
    }
}
