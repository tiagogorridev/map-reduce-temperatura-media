package br.pucpr.bigdata.correlacao;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Le cada linha do CSV e envia, para cada coluna, os valores que a formula da correlacao precisa
public class CorrelacaoMap extends Mapper<LongWritable, Text, Text, PearsonWritable> {

    private static final int INDICE_TEMPERATURA = 8;
    private static final int TOTAL_COLUNAS = 13;

    private static final String[] NOMES_COLUNAS = {
        "X", "Y", "month", "day", "FFMC", "DMC", "DC", "ISI",
        "temp", "RH", "wind", "rain", "area"
    };

    // colunas comparadas com a temperatura (month e day ficam de fora porque sao texto)
    private static final int[] INDICES_COMPARADOS = {0, 1, 4, 5, 6, 7, 9, 10, 11, 12};

    private static final Text chaveColuna = new Text();
    private static final PearsonWritable valorComposto = new PearsonWritable();

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

        for (int indice : INDICES_COMPARADOS) {
            double x;
            try {
                x = Double.parseDouble(colunas[indice].trim());
            } catch (NumberFormatException e) {
                continue;
            }

            // envia o nome da coluna junto com o valor dela e a temperatura da mesma linha
            chaveColuna.set(NOMES_COLUNAS[indice]);
            valorComposto.set(x, temperatura);
            context.write(chaveColuna, valorComposto);
        }
    }
}
