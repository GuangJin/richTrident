/** 
 * @file RichTridentTopology.java
 * @class com.jin.storm.richTrident.RichTridentTopology
 * @brief This is a subclass of TridentTopology to support more features.
 * @note The purpose here is to subclass TridentTopology to create new features.
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 * 
 **/
package com.jin.storm.richTrident;

import java.util.Arrays;
import java.util.List;

import backtype.storm.ILocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.planner.SpoutNode;
import storm.trident.spout.IBatchSpout;
import storm.trident.util.TridentUtils;

public class RichTridentTopology extends TridentTopology{
	
	/**
	 * @brief create a RichStream from a spout. 
	 * @param txId	The id of the RichStream. 
	 * @param spout	The input spout.
	 * @return A RichStream representing the query results.
	 **/
	public RichStream newRichStream(String txId, IBatchSpout spout){
		Node n = new SpoutNode(getUniqueStreamId(), spout.getOutputFields(), txId, spout, SpoutNode.SpoutType.BATCH);
		registerNode(n);//from super class
        return new RichStream(this, n.name, n);
	}
	
	/**
	 * @brief Register a new node to work on a list of source streams. 
	 * @param sources	The streams for the node to work on. 
	 * @param newNode	The worker node.
	 * @return A RichStream representing result.
	 **/
    protected RichStream addSourcedNode(List<Stream> sources, Node newNode) {
        registerSourcedNode(sources, newNode);
        return new RichStream(this, newNode.name, newNode);
    }
    
	/**
	 * @brief Register a new node to work on a source stream. 
	 * @param source	The stream for the node to work on. 
	 * @param newNode	The worker node.
	 * @return A RichStream representing result.
	 **/
    protected RichStream addSourcedNode(Stream source, Node newNode) {
        return addSourcedNode(Arrays.asList(source), newNode);
    }
    
	/**
	 * @brief Wrapper of the super class to generate a unique stream id. 
	 * @return A unique stream id in String.
	 **/
    public String getUniqueStreamId() {
        return super.getUniqueStreamId();
    }
    
	/**
	 * @brief Generate a RichStream from a LocalDRPC. 
	 * @param function	The name for the DRPC.
	 * @param server	The LocalDRPC.
	 * @return A RichStream for the DRPCStream.
	 **/
    public RichStream newDRPCStream(String function, ILocalDRPC server) {
        DRPCSpout spout;
        if(server==null) {
            spout = new DRPCSpout(function);
        } else {
            spout = new DRPCSpout(function, server);
        }
        return newDRPCStream(spout);
    }
    
	/**
	 * @brief Register a node to current topology.
	 * @param n	The Node.
	 * @return A RichStream representing Node n.
	 **/
    protected RichStream addNode(Node n) {
        registerNode(n);
        return new RichStream(this, n.name, n);
    }
    
	/**
	 * @brief Create a RichStream for a DRPCSpout.
	 * @param spout	The DRPCSpout.
	 * @return A RichStream representing the spout.
	 **/
    private RichStream newDRPCStream(DRPCSpout spout) {
        //return RichStream instead of Stream
        Node n = new SpoutNode(getUniqueStreamId(), TridentUtils.getSingleOutputStreamFields(spout), null, spout, SpoutNode.SpoutType.DRPC);
        RichStream nextStream = addNode(n);//get a Stream first
        RichStream outputStream = nextStream.project(new Fields("args"));
        return new RichStream(this, outputStream._node.name, outputStream._node);
    }
}
