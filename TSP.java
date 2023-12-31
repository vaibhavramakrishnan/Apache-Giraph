package giraph;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.giraph.comm.ArrayListWritable;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Shows an example of a brute-force implementation of the Travelling Salesman Problem
 */
public class TSP extends
        EdgeListVertex<LongWritable, ArrayListWritable<DoubleWritable>,
        FloatWritable, ArrayListWritable<Text>> implements Tool {
    /** Configuration */
    private Configuration conf;
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(TSP.class);
    /** The shortest paths id */
    public static String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;
    

    /**
     * Is this vertex the source id?
     *
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() ==
            getContext().getConfiguration().getLong(SOURCE_ID,
                                                    SOURCE_ID_DEFAULT));
    }
    public class Message extends ArrayListWritable<Text> {
 	   public Message() {
 	     super();
 	   }

	@Override
		public void setClass() {
		// TODO Auto-generated method stub
		
		}
 	 }
    public class Valeur extends ArrayListWritable<DoubleWritable> {
  	   public Valeur() {
  	     super();
  	   }

 	@Override
 		public void setClass() {
 		// TODO Auto-generated method stub
 		
 		}
  	 }

    @Override
    public void compute(Iterator<ArrayListWritable<Text>> msgIterator) {
    	System.out.println("****     LAUNCHING COMPUTATION FOR VERTEX "+this.getVertexId().get()+", SUPERSTEP "+this.getSuperstep()+"      ****");
    	//We get the source ID, we will need it
    	String sourceID = new LongWritable(this.getContext().getConfiguration().getLong(SOURCE_ID,
                SOURCE_ID_DEFAULT)).toString();
    	//We get the total number of verticles, and the current superstep number, we will need it too
        int J=1;
		for (LongWritable targetVertexId : this) {
			J++;
		}
		final int N=J;
    	long k=this.getSuperstep();
    	/** 
    	 * COLLECTING OF THE PREVIOUS MESSAGES :
    	 * We need to collect the previous superstep's messages :
    	 * the positive numbers will go to the distance list, and the negative ones will go to the path list
    	 */
    	//we first collect all messages in one list, each message is a pair (path,cost)
        ArrayList<ArrayListWritable<Text>> messages=new ArrayList<ArrayListWritable<Text>>();
        ArrayList<IntWritable> idList=new ArrayList<IntWritable>();
        ArrayList<DoubleWritable> distList=new ArrayList<DoubleWritable>();
        //we then fill the two lists : the path list and the distance list
        while (msgIterator.hasNext()){
        	ArrayListWritable<Text> message = msgIterator.next();
        	messages.add(message);
        	idList.add(new IntWritable(Integer.parseInt(message.get(0).toString())));
    		System.out.println("Adding the path "+message.get(0).toString()+" to the paths list");
    		distList.add(new DoubleWritable(Double.parseDouble(message.get(1).toString())));
    		System.out.println("Adding the distance "+message.get(1).toString()+" to the distance list");
        }
        
        	
        
        /**
         * NORMAL SUPERSTEP>0
         */
        if((k<N-1 && k>0) || k==1){
        	System.out.println("NORMAL SUPERSTEP "+this.getSuperstep());
        	//we will process the received messages one by one
        	int index = 0;//the current message number (for the distance and the path)
            String path=sourceID;
            for (DoubleWritable message : distList){
            	//we can only send messages to vertex that are not already in the path, so
            	//we have to know which vertexs are already in the paths
            	path=idList.get(index).toString();
            	ArrayList liste = new ArrayList();
				StringTokenizer tokens = new StringTokenizer(path,"");
				while(tokens.hasMoreTokens()){
					liste.add(tokens.nextElement());
            	}
            	ArrayList<String> listeString=new ArrayList<String>();
            	for(Object token:liste){
            	 	listeString.add(token.toString());
            	}      		
            	//we can now process all edges and send to the target vertex only if it is not in the path already:
            	for (LongWritable targetVertexId : this) {
        			System.out.println("We will now try to send a message to the vertex "+targetVertexId.toString()+" , here is a list of the vertex already done : ");
            		boolean isAlreadyInThePath=false;
            		for(String vertex:listeString){
            			if(vertex.contains(targetVertexId.toString())){
            				isAlreadyInThePath=true;
            			}
            		}
            		if(isAlreadyInThePath==true){
            				System.out.println("This vertex "+targetVertexId.toString()+" is already in the path : "+listeString);
            		}
            		if(isAlreadyInThePath==false){
    					//we can send the two messages to this target vertex : the new distance (positive) and the new path (negative)
            			Message msg = new Message();
            			//first message : the new distance cost
       					int dist=(int) Math.floor(Double.parseDouble(message.toString()));
       					float value = dist+this.getEdgeValue(targetVertexId).get();
            			String newPath = path+this.getVertexId().toString();
            			msg.add(new Text(newPath));
            			msg.add(new Text(String.valueOf(value)));
            			this.sendMsg(targetVertexId,msg);
            			System.out.println("We send the message "+msg.toString()+"to the vertex "+targetVertexId.toString());
            		}
            			
            	}
            	
            index++;
            }
            System.out.println("END OF SUPERSTEP "+k);
        }
        /**
         * LAST SUPERSTEP BUT ONE :
         * At the last superstep but one, we send all the messages to the source vertex
         */
        if(k==N-1 && k>1){
        	System.out.println("SUPERSTEP N-1");
        	if(this.getVertexId().toString().equals(sourceID)){
        		System.out.println("This is the source vertex, this message should not appear");
        	}
        	else{
                int index2 = 0;//the current message's index
                String newPath=sourceID;
                for (DoubleWritable message : distList){
                	newPath=idList.get(index2).toString();
                	LongWritable sourceIDLongWritable= new LongWritable(Long.parseLong(sourceID));
                	Message msg=new Message();
                	//first message : the new distance cost
                	double dist=Double.parseDouble(message.toString());
                	double value = dist+this.getEdgeValue(sourceIDLongWritable).get();
                	newPath=newPath+this.getVertexId().toString();
                	msg.add(new Text(newPath));
                	msg.add(new Text(String.valueOf(value)));
                	this.sendMsg(sourceIDLongWritable,msg);
                	System.out.println("We send the path "+msg.toString()+" to the vertex "+sourceIDLongWritable.toString());
                	index2++;
                }
        	}
            System.out.println("END OF SUPERSTEP N-1");
        }
        
        
        /**
         * LAST SUPERSTEP :
         * The source vertex retrieves all the possible paths, and can then find the shortest
         */
        if(k==N && k>1 && this.getVertexId().toString().equals(sourceID)){
            System.out.println("LAST SUPERSTEP");
        	//we look for the minimu distance cost among the distance messages, and also take the corresponding path
        	double minDist = Double.MAX_VALUE;
        	int index3 = 0;
        	int finalIndex=0;
        	for(DoubleWritable message : distList){
        		double valeurDuMessage = Double.parseDouble(message.toString());
        		if(minDist>valeurDuMessage){
        			minDist=valeurDuMessage;
        			finalIndex=index3;
        		}
        	index3++;
        	}
        	//we can then access the shortest Path
        	String finalPath="";
        	finalPath=idList.get(finalIndex).toString()+sourceID;
        	DoubleWritable idFinal = new DoubleWritable(Double.parseDouble(finalPath));
        	Valeur valeur = new Valeur();
			valeur.add(idFinal);
			valeur.add(new DoubleWritable(minDist));
			this.setVertexValue(valeur);
        	System.out.println("The shortest path is "+finalPath+" , costing "+minDist);
        	System.out.println("END OF THE LAST SUPERSTEP");
        }
        System.out.println("***   END OF THE SUPERSTEP "+k+" FOR THE VERTEX "+this.getVertexId().toString()+" VOTES TO HALT     *****");
        voteToHalt();
    }

    /**
     * VertexInputFormat that supports {@link TSP}
     */
    public static class SimpleShortestPathsVertexInputFormat extends
            TextVertexInputFormat<LongWritable,
            ArrayListWritable<DoubleWritable>,
                                  FloatWritable,
                                  DoubleWritable> {
        @Override
        public VertexReader<LongWritable, ArrayListWritable<DoubleWritable>, FloatWritable, DoubleWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new SimpleShortestPathsVertexReader(
                textInputFormat.createRecordReader(split, context));
        }
    }

    /**
     * VertexReader that supports {@link TSP}.  In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
    public static class SimpleShortestPathsVertexReader extends
            TextVertexReader<LongWritable,
            ArrayListWritable<DoubleWritable>, FloatWritable, DoubleWritable> {

        public SimpleShortestPathsVertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        public class Valeur extends ArrayListWritable<DoubleWritable> {
       	   public Valeur() {
       	     super();
       	   }

      	@Override
      		public void setClass() {
      		// TODO Auto-generated method stub
      		
      		}
       	 }
        
        @Override
        public BasicVertex<LongWritable, ArrayListWritable<DoubleWritable>, FloatWritable,
                           DoubleWritable> getCurrentVertex()
            throws IOException, InterruptedException {
          BasicVertex<LongWritable, ArrayListWritable<DoubleWritable>, FloatWritable,
              DoubleWritable> vertex = BspUtils.<LongWritable, ArrayListWritable<DoubleWritable>, FloatWritable,
                  DoubleWritable>createVertex(getContext().getConfiguration());

            Text line = getRecordReader().getCurrentValue();
            try {
                JSONArray jsonVertex = new JSONArray(line.toString());
                LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
               Valeur vertexValue = new Valeur();
               vertexValue.add(new DoubleWritable(jsonVertex.getDouble(1)));
                Map<LongWritable, FloatWritable> edges = Maps.newHashMap();
                JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
                for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                    JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                    edges.put(new LongWritable(jsonEdge.getLong(0)),
                            new FloatWritable((float) jsonEdge.getDouble(1)));
                }
                vertex.initialize(vertexId, vertexValue, edges, null);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Couldn't get vertex from line " + line.toString(), e);
            }
            return vertex;
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }
    }

    /**
     * VertexOutputFormat that supports {@link TSP}
     */
    public static class SimpleShortestPathsVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, ArrayListWritable<DoubleWritable>,
            FloatWritable> {

        @Override
        public VertexWriter<LongWritable, ArrayListWritable<DoubleWritable>, FloatWritable>
                createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new SimpleShortestPathsVertexWriter(recordWriter);
        }
    }

    /**
     * VertexWriter that supports {@link TSP}
     */
    public static class SimpleShortestPathsVertexWriter extends
            TextVertexWriter<LongWritable, ArrayListWritable<DoubleWritable>, FloatWritable> {
        public SimpleShortestPathsVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<LongWritable, ArrayListWritable<DoubleWritable>,
                                FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
        	String sourceID = new LongWritable(vertex.getContext().getConfiguration().getLong(SOURCE_ID,
                    SOURCE_ID_DEFAULT)).toString();
        	JSONArray jsonVertex = new JSONArray();
            try {
                jsonVertex.put(vertex.getVertexId().get());
                jsonVertex.put(vertex.getVertexValue().toString());
                JSONArray jsonEdgeArray = new JSONArray();
                for (LongWritable targetVertexId : vertex) {
                    JSONArray jsonEdge = new JSONArray();
                    jsonEdge.put(targetVertexId.get());
                    jsonEdge.put(vertex.getEdgeValue(targetVertexId).get());
                    jsonEdgeArray.put(jsonEdge);
                }
                jsonVertex.put(jsonEdgeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "writeVertex: Couldn't write vertex " + vertex);
            }
            getRecordWriter().write(new Text(jsonVertex.toString()), null);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] argArray) throws Exception {
        Preconditions.checkArgument(argArray.length == 4,
            "run: Must have 4 arguments <input path> <output path> " +
            "<source vertex id> <# of workers>");

        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.setVertexClass(getClass());
        job.setVertexInputFormatClass(
            SimpleShortestPathsVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            SimpleShortestPathsVertexOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        job.getConfiguration().setLong(TSP.SOURCE_ID,
                                       Long.parseLong(argArray[2]));
        job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
                                   Integer.parseInt(argArray[3]),
                                   100.0f);

        return job.run(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TSP(), args));
    }
}

