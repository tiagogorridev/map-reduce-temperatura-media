package br.pucpr.bigdata.correlacao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// junta as somas de todas as maquinas e calcula a correlacao de cada coluna com a temperatura
public class CorrelacaoReduce extends Reducer<Text, PearsonWritable, Text, DoubleWritable> {

    // guarda a correlacao de cada coluna para ordenar no final
    private final Map<String, Double> resultados = new HashMap<>();

    @Override
    protected void reduce(Text chave, Iterable<PearsonWritable> valores, Context context)
            throws IOException, InterruptedException {

        PearsonWritable total = new PearsonWritable();
        for (PearsonWritable valor : valores) {
            total.somar(valor);
        }

        double r = total.correlacao();
        if (total.getN() > 1 && !Double.isNaN(r)) {
            resultados.put(chave.toString(), r);
        }
    }

    // roda uma vez no final e escreve da maior relacao para a menor
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<String, Double>> ordenados = new ArrayList<>(resultados.entrySet());
        ordenados.sort((a, b) -> Double.compare(Math.abs(b.getValue()), Math.abs(a.getValue())));

        for (Map.Entry<String, Double> resultado : ordenados) {
            context.write(new Text(resultado.getKey()), new DoubleWritable(resultado.getValue()));
        }
    }
}
