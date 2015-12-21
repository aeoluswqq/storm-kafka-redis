package storm.kafka;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.Spout.WordReader;
import storm.kafka.bolt.WordCounter;
import storm.kafka.bolt.WordNormalizer;
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
    //定义 Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));
    //配置 Topology
        Config conf = new Config();
        conf.put("wordsFile", "d:/a.txt");
        conf.setDebug(false);
    //本地模式运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }

}