package br.pucpr.bigdata.correlacao;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// guarda as somas que a formula da correlacao precisa (x = outra coluna, y = temperatura)
public class PearsonWritable implements Writable {

    private long n;
    private double somaX;
    private double somaY;
    private double somaX2;
    private double somaY2;
    private double somaXY;

    public PearsonWritable() {
    }

    // preenche com os valores de uma linha do CSV
    public void set(double x, double y) {
        n = 1;
        somaX = x;
        somaY = y;
        somaX2 = x * x;
        somaY2 = y * y;
        somaXY = x * y;
    }

    // soma os valores de outro objeto neste (usado no Combiner e no Reduce)
    public void somar(PearsonWritable outro) {
        n += outro.n;
        somaX += outro.somaX;
        somaY += outro.somaY;
        somaX2 += outro.somaX2;
        somaY2 += outro.somaY2;
        somaXY += outro.somaXY;
    }

    public long getN() {
        return n;
    }

    // calcula a correlacao uma unica vez, com as somas de todas as linhas
    public double correlacao() {
        double numerador = n * somaXY - somaX * somaY;
        double denominador = Math.sqrt(n * somaX2 - somaX * somaX)
                * Math.sqrt(n * somaY2 - somaY * somaY);
        return denominador == 0 ? Double.NaN : numerador / denominador;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(n);
        out.writeDouble(somaX);
        out.writeDouble(somaY);
        out.writeDouble(somaX2);
        out.writeDouble(somaY2);
        out.writeDouble(somaXY);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = in.readLong();
        somaX = in.readDouble();
        somaY = in.readDouble();
        somaX2 = in.readDouble();
        somaY2 = in.readDouble();
        somaXY = in.readDouble();
    }

    @Override
    public String toString() {
        return n + "\t" + somaX + "\t" + somaY + "\t" + somaX2 + "\t" + somaY2 + "\t" + somaXY;
    }
}
