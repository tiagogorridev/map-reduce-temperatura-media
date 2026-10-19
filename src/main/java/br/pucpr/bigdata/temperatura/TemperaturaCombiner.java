package br.pucpr.bigdata.temperatura;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperaturaCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text chave, Iterable<Text> valores, Context context)
            throws IOException, InterruptedException {

        double somaParcial = 0;
        long quantidadeParcial = 0;

        for (Text valor : valores) {
            String[] partes = valor.toString().split(";");
            somaParcial += Double.parseDouble(partes[0]);
            quantidadeParcial += Long.parseLong(partes[1]);
        }

        context.write(chave, new Text(somaParcial + ";" + quantidadeParcial));
    }
}