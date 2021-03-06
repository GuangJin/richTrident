/** 
 * @file NextLargerValue.java
 * @class richTrident.examples.NextLargerValue
 * @brief This is an example showing how to use RichStream to get the next larger value.
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 **/
package richTrident.examples;

import java.util.TreeMap;

import com.jin.storm.richTrident.RichTridentTopology;
import com.jin.storm.richTrident.operation.NextValuePostRichQueryFunction;
import com.jin.storm.richTrident.operation.NextValuePreRichQueryFunction;

import storm.trident.TridentState;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class NextLargerValue {
	
	  public static class Split extends BaseFunction {
		    @Override
		    public void execute(TridentTuple tuple, TridentCollector collector) {
		      String sentence = tuple.getString(0);
		      for (String word : sentence.split(" ")) {
		        collector.emit(new Values(new Integer(word)));
		      }
		    }
		  }

	public static StormTopology buildTopology(LocalDRPC drpc) {
		RichTridentTopology topology = new RichTridentTopology();
	    
	    String[] fieldNames = {"id","value"};
		FixedBatchSpout studentSpout = new FixedBatchSpout(new Fields(fieldNames), 4,
				new Values("Mark",new Integer(1)), new Values("Smith",new Integer(2)), 
				new Values("John",new Integer(3)), new Values("Mary",new Integer(4)),
				new Values("Tom",new Integer(5)), new Values("Brittany", new Integer(6)));
		studentSpout.setCycle(true);//a spout to generate id and value pairs

		TridentState idBasedState = topology.newRichStream("student", studentSpout)//one stream for a tuples with id and value
										.distributePersist(new Fields("id"), new Fields("value"), new TreeMap())//distribute the tuples based on their ids and index the value by a TreeMap (a black-red tree actually)
										.parallelismHint(5);//5 worker nodes running to maintain the state
		
		topology.newDRPCStream("drpc", drpc).each(new Fields("args"), new Split(), new Fields("value"))//another stream is generated by a drpc through its args(see Split function which parse the input args string into value)
				.stateRichQuery(idBasedState,//query the state  
						new Fields("value"),//the drpc stream provides the value field 
						new NextValuePreRichQueryFunction(), //preQuery gets the coarse results
						new NextValuePostRichQueryFunction(new Fields("value")),//postQuery refines based on the value field from studentSpout's tuples
						new Fields("nextValue"));//generate a new field for the final results 
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("richTrident", conf, buildTopology(drpc));
			for (int i = 0; i < 100; i++) {
				System.out.println("DRPC RESULT: "+ drpc.execute("drpc", "1 2 1000"));
				Thread.sleep(1000);
			}
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					buildTopology(null));
		}
	}
}