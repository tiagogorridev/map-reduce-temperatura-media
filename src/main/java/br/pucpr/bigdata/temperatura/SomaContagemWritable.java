package br.pucpr.bigdata.temperatura;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// Guarda a soma das temperaturas e a quantidade de linhas somadas
public class SomaContagemWritable implements Writable {

    private double soma;
    private long quantidade;

    public SomaContagemWritable() {
    }

    public SomaContagemWritable(double soma, long quantidade) {
        this.soma = soma;
        this.quantidade = quantidade;
    }

    public void set(double soma, long quantidade) {
        this.soma = soma;
        this.quantidade = quantidade;
    }

    public double getSoma() {
        return soma;
    }

    public long getQuantidade() {
        return quantidade;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(quantidade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        quantidade = in.readLong();
    }

    @Override
    public String toString() {
        return soma + "\t" + quantidade;
    }
}
