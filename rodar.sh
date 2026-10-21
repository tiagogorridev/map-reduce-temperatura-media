set -e

mvn package
rm -rf saida-media saida-correlacao

hadoop jar target/temperatura-media-1.0.jar br.pucpr.bigdata.temperatura.TemperaturaMain input/forestfireinput.csv saida-media
hadoop jar target/temperatura-media-1.0.jar br.pucpr.bigdata.correlacao.CorrelacaoMain input/forestfireinput.csv saida-correlacao

echo
echo "=== Media ==="
cat saida-media/part-r-00000
echo
echo "=== Correlacao ==="
cat saida-correlacao/part-r-00000
