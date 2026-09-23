package br.pucpr.bigdata.temperatura;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperaturaMap extends Mapper<LongWritable, Text, Text, Text> {

    private static final int INDICE_TEMPERATURA = 8;
    private static final int TOTAL_COLUNAS = 13;

    private static final Text chaveGlobal = new Text("temperatura");
    private static final Text valorComposto = new Text();

    @Override
    protected void map(LongWritable offset, Text linha, Context context)
        throws IOException, InterruptedException {

            String[] colunas = linha.toString().split(",");
            if (colunas.length != TOTAL_COLUNAS) {
                return;
            }

            double temperatura;
            try {
                temperatura = Double.parseDouble(colunas[INDICE_TEMPERATURA].trim());
            } catch (NumberFormatException e) {
                return;
            }

            valorComposto.set(temperatura + ";1");
            context.write(chaveGlobal, valorComposto);
        }

}