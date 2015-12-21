package storm.kafka.bolt;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import storm.kafka.tools.RedisUtil;

public class WordCounter implements IRichBolt {
	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;
	static String NEWSDAYPVUVPREFIX = "newsdaypvuv";// 每天的新闻总pv和uv
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		RedisUtil.getPool();
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

	}
	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		// 确认成功处理一个tuple
		collector.ack(input);
		String time = new SimpleDateFormat("yyyyMMdd").format(new Date());
		String pvuvkey=NEWSDAYPVUVPREFIX+":"+time;
		RedisUtil.SortedsetIncrby(pvuvkey, 1,"pv");
	}
	/**
	 * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
	 * 因为这只是个Demo，我们用它来打印我们的计数器
	 * */
	@Override
	public void cleanup() {
//		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
//		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
//			System.err.println(entry.getKey() + ": " + entry.getValue());
//		}
//		counters.clear();
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}