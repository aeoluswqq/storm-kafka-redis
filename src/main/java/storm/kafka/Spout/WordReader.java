package storm.kafka.Spout;
import java.io.BufferedReader;  
        import java.io.FileNotFoundException;  
        import java.io.FileReader;  
        import java.util.Map;  
        import backtype.storm.spout.SpoutOutputCollector;  
        import backtype.storm.task.TopologyContext;  
        import backtype.storm.topology.IRichSpout;  
        import backtype.storm.topology.OutputFieldsDeclarer;  
        import backtype.storm.tuple.Fields;  
        import backtype.storm.tuple.Values;  
        public class WordReader implements IRichSpout {  
            private SpoutOutputCollector collector;  
            private FileReader fileReader;  
            private boolean completed = false;  
            private TopologyContext context;  
            public boolean isDistributed() {return false;}  
            public void ack(Object msgId) {  
                    System.out.println("OK:"+msgId);  
            }  
            public void close()  {}  
            public void fail(Object msgId) {  
                 System.out.println("FAIL:"+msgId);  
            }  
  
            public void nextTuple() {  
                 if(completed){  
                     try {  
                         Thread.sleep(1000);  
                     } catch (InterruptedException e) {  
                         //什么也不做  
                     }  
                    return;  
                 }  
                 String str;  
                 //创建reader  
                 BufferedReader reader = new BufferedReader(fileReader);  
                 try{  
                     //读所有文本行  
                    while((str = reader.readLine()) != null){  
                     /** 
                      * 按行发布一个新值 
                      */  
                         this.collector.emit(new Values(str),str);  
                     }  
                 }catch(Exception e){  
                     throw new RuntimeException("Error reading tuple",e);  
                 }finally{  
                     completed = true;  
                 }  
             }  
  
             public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {  
                     try {  
                         this.context = context;  
                         this.fileReader = new FileReader(conf.get("wordsFile").toString());  
                     } catch (FileNotFoundException e) {  
                         throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");  
                     }  
                     this.collector = collector;  
             }  
  
             public void declareOutputFields(OutputFieldsDeclarer declarer) {  
                 declarer.declare(new Fields("line"));  
             }
			@Override
			public void activate() {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void deactivate() {
				// TODO Auto-generated method stub
				
			}
			@Override
			public Map<String, Object> getComponentConfiguration() {
				// TODO Auto-generated method stub
				return null;
			}  
        } 