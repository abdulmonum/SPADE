/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2020 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------
 */
package spade.filter;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.File;

import spade.core.AbstractEdge;
import spade.core.AbstractVertex;
import spade.core.Kernel;
import spade.core.Settings;
import spade.query.quickgrail.core.GraphStatistic;
import spade.storage.PostgreSQL;
import spade.utility.HelperFunctions;
import spade.utility.RemoteSPADEQueryConnection;
import spade.utility.FileUtility;

import spade.core.Graph;
import spade.core.Query;
import spade.query.quickgrail.instruction.SaveGraph;


/*
  A class to receive CrossNamespace events (https://github.com/ashish-gehani/SPADE/wiki/Available-filters#crossnamespaces)
  in real-time.

  All available information is provided to the function 'handleCrossNamespaceEvent'.

  In addition, the class provides a way of querying SPADE storages (like from the query client). This can be used
  to extract more information about CrossNamespaces events. This functionality is provided through the class member
  'queryClient'.

  How to use this class:

  From the SPADE control client, use the command 'add filter QueryableCrossNamespaces position=1'

  Note that, the CrossNamespaces filter is not required because this class is a child of CrossNamespaces filter i.e.
  all the functionality of CrossNamespaces is automatically included when QueryableCrossNamespaces filter is added.

  The config file 'cfg/spade.filter.CrossNamespaces.config' is still valid and in effect when this filter is used.


  For any configuration specific to this filter, use the file 'cfg/spade.filter.QueryableCrossNamespaces.config'.
*/
public class QueryableCrossNamespaces extends CrossNamespaces{

	private static final Logger logger = Logger.getLogger(QueryableCrossNamespaces.class.getName());

	private static final String
			keyExportFilePath = "exportPath",
			keyReporter = "reporter";


	private static String ABSOLUTE_EXPORT_PATH = null;

	private static boolean CAMFLOW_TRUE_AUDIT_FALSE;
	
	private HashMap<String,ArrayList<String>> ARTIFACT_READERS_MAP;

        private HashMap<String,ArrayList<String>> ENTITY_TO_READER_WRITER;

	private RemoteSPADEQueryConnection queryClient;

	@Override
	public boolean initialize(final String arguments){
			if(!super.initialize(arguments)){
					return false;
			}
			final String thisConfigFilePath = Settings.getDefaultConfigFilePath(QueryableCrossNamespaces.class);
			final String parentConfigFilePath = Settings.getDefaultConfigFilePath(CrossNamespaces.class);
			try{
					final Map<String, String> configMap = HelperFunctions.parseKeyValuePairsFrom(
									arguments, new String[]{thisConfigFilePath, parentConfigFilePath}
					);
					logger.log(Level.INFO, "configFilePath: {0}", thisConfigFilePath);
					logger.log(Level.INFO, "parentFilePath: {0}", parentConfigFilePath);

					queryClient = new RemoteSPADEQueryConnection(Kernel.getHostName(), "localhost", Settings.getCommandLineQueryPort());
					queryClient.connect(Kernel.getClientSocketFactory(), 5 * 1000);
					// Set the storage appropriately i.e. based on the one that is being used
					queryClient.setStorage(PostgreSQL.class.getSimpleName());

					// Log any arguments that you get from 'configMap' TODO
					logger.log(Level.INFO, "Arguments: {0}", configMap);

					// extracting export path
					ABSOLUTE_EXPORT_PATH = configMap.get(keyExportFilePath);

					// extracting audit or camflow
					if(configMap.get(keyReporter).equalsIgnoreCase("audit")){
							CAMFLOW_TRUE_AUDIT_FALSE = false;
					} else if(configMap.get(keyReporter).equalsIgnoreCase("camflow")){
							CAMFLOW_TRUE_AUDIT_FALSE = true;
					} else{
							logger.log(Level.INFO, "invalid reporter: {0}", configMap.get(keyReporter));
							throw new Exception("reporter needs to be either camflow or audit.");
					}
					
					// initializing ARTIFACT_READERS_MAP
					ARTIFACT_READERS_MAP = new HashMap<String,ArrayList<String>>();

                                        // initializing ENTITY_TO_READER_WRITER
                                        ENTITY_TO_READER_WRITER = new HashMap<String,ArrayList<String>>();


                }catch(Exception e){
                        logger.log(Level.SEVERE, "Failed to initialize", e);
                        return false;
                }
                return true;
        }

        @Override
        public boolean shutdown(){
                super.shutdown();
                // Cleanup (if required) TODO
                try{
                        // --- transformed graph generation ---
                        remainingQueriesCamflow();
                        queryClient.close();
                }catch(Exception e){
                        logger.log(Level.WARNING, "Failed to close remote query connection", e);
                }
                return true;
        }

        @Override
        public void handleCrossNamespaceEvent(final long eventId,
                        final TreeMap<String, String> matchedArtifactAnnotations,
                        final HashSet<TreeMap<String, String>> completeArtifactAnnotationsSet,
                        final HashSet<TreeMap<String, String>> completeOtherWriters,
                        final AbstractVertex readerProcessVertex,
                        final AbstractEdge readEdge){
                try{
                        super.handleCrossNamespaceEvent(
                                        eventId, matchedArtifactAnnotations, completeArtifactAnnotationsSet
                                        , completeOtherWriters, readerProcessVertex, readEdge
                                        );
                }catch(Exception e){
                        logger.log(Level.WARNING, "Parent failed to handle event", e);
                        return;
                }
                // Handle the event as required. Wiki (for arguments): https://github.com/ashish-gehani/SPADE/wiki/Available-filters#crossnamespaces
                try{
                        // Add query code here TODO

                        /*
                         * Execute an arbitrary query
                         * Exception thrown if an error occured while querying
                         *
                         * All queries (that can executed on the query client) are valid here. The only exception are:
                         * 1) 'export > path_to_dot_file'
                         * 2) 'load path_to_query_file'
                        */

                        // method: queryClient.executeQuery("");

                        // ------ CAMFLOW QUERY ------

                        // queryClient.executeQuery("reset workspace");

                        // --- entity constraint ---
                        // extracting matched annotations and making their constraints
                        String graphName = "";
                        String entityConstraint = "";

                        for(Map.Entry<String,String> entry : matchedArtifactAnnotations.entrySet()) {
                                String key = entry.getKey();
                                String value = entry.getValue();

                                entityConstraint += " \"" + key + "\" == '" + value + "' and";
                                graphName += value + "_";
                        }
                        entityConstraint = entityConstraint.substring(0, entityConstraint.length() - 4);
                        String templateName = graphName.replaceAll("[^a-zA-Z0-9]", "");

                        graphName += "graph.dot";


                        queryClient.executeQuery("%entity_constraint =" + entityConstraint);

                        logger.log(Level.INFO, "Entity Constraint: {0}, Event id: {1}", new Object[]{entityConstraint, eventId});

                        // getting crossnamepace entities
                        queryClient.executeQuery("$crossnamespace_entities"+ templateName +" = $base.getVertex(%entity_constraint)");

			// --- crossnamespace readers ---
			ArrayList<String> readerConstraintList = getReaderConstraintList(readEdge, graphName);
                        String readerCrossnamespaceVariable = makeReaderEntitiesCamflow(readerConstraintList, templateName);

                        // --- crossnamespace writers ---
                        ArrayList<String> writerConstraintList = getWriterConstraintList(completeOtherWriters);
                        String writerCrossnamespaceVariable = makeWriterEntitiesCamflow(writerConstraintList, templateName);

                        ArrayList<String> readerAndWriter = new ArrayList<String>();
                        readerAndWriter.add(readerCrossnamespaceVariable);
                        readerAndWriter.add(writerCrossnamespaceVariable);

                        ENTITY_TO_READER_WRITER.put("$crossnamespace_entities" + templateName, readerAndWriter);

                        
                        

                        /*
                         * Export a graph
                         *
                         * Do not export big graphs because of memory constraint
                         *
                         * This is the same Graph class that is seen in Transformers
                         */
                        final String symbol = "$transformed_subgraph"; // Name of the graph symbol to export
                        final boolean force = true; // Export even if graph is big
                        final boolean verify = false; // Do not verify the query response
                        final spade.core.Graph graph = queryClient.exportGraph(symbol, force, verify);

                        // Exporting graph to a dot file
                        String completeFilePath = ABSOLUTE_EXPORT_PATH + File.separator + graphName;
                        Graph.exportGraphToFile(SaveGraph.Format.kDot, completeFilePath, graph);

                        /*
                         * Check size of graph
                         */
                        final GraphStatistic.Count count = queryClient.getGraphCount(symbol);
                        if(count.getVertices() > 0 || count.getEdges() > 0){
                                // Not empty
                        }
                }catch(Exception e){
                        logger.log(Level.WARNING, "Error in querying", e);
                }
        }

	public String makeReaderEntitiesCamflow(ArrayList<String> readerConstraintList, String templateName){
		try{
                        // executing readers constraints
                        String chainedReaderConstraint = "";
                        int total_reader_constraints = readerConstraintList.size();
                        for(int i = 0; i <= total_reader_constraints; i++){
                                queryClient.executeQuery(readerConstraintList.get(i));
                                chainedReaderConstraint += "%reader_constraint" + String.valueOf(i) + " or ";
                        }
                        logger.log(Level.INFO, "Chained Reader Constraint: {0}", chainedReaderConstraint);
                        if(chainedReaderConstraint.length() > 0){
                                chainedReaderConstraint = chainedReaderConstraint.substring(0, chainedReaderConstraint.length() - 4);
                        }

                        // getting reader entities
                        queryClient.executeQuery("$crossnamespace_readers" + templateName + " = $base.getVertex(" + chainedReaderConstraint + ")");

		}catch(Exception e){
                        logger.log(Level.WARNING, "Error in querying", e);
                }

                return "$crossnamespace_readers" + templateName;
		
	}

        public ArrayList<String> getReaderConstraintList(AbstractEdge readEdge, String graphName){
                // --- reader constraint ---
                // adding artifact and reader to map

                ArrayList<String> readers = ARTIFACT_READERS_MAP.get(graphName);

                if(readers == null){
                        readers = new ArrayList<String>();
                        readers.add(readEdge.getAnnotation("id"));
                        ARTIFACT_READERS_MAP.put(graphName, readers);
                } else{
                        readers.add(readEdge.getAnnotation("id"));
                        ARTIFACT_READERS_MAP.put(graphName, readers);
                }


                // creating reader ids
                int total_reader_constraints = 0;

                String readerConstraint = "%reader_constraint" + String.valueOf(total_reader_constraints) + " =";
                ArrayList<String> readerConstraintList = new ArrayList<String>();
                readerConstraintList.add(readerConstraint);

                // creating reader constriants, splitting into multiple constriants
                for(int i = 0; i < readers.size(); i++){
                        String currentConstraint = readerConstraintList.get(total_reader_constraints);
                        currentConstraint += " \"id\" == '" + readers.get(i) + "' and";
                        readerConstraintList.set(total_reader_constraints, currentConstraint);

                        if((i != 0) && (i%6 == 0)){
                                currentConstraint = readerConstraintList.get(total_reader_constraints);
                                currentConstraint = currentConstraint.substring(0, currentConstraint.length() - 4);
                                readerConstraintList.set(total_reader_constraints, currentConstraint);

                                total_reader_constraints++;
                                
                                readerConstraint = "%reader_constraint" + String.valueOf(total_reader_constraints) + " =";
                                readerConstraintList.add(readerConstraint);
                        }
                }

                if(readerConstraintList.get(total_reader_constraints).substring(readerConstraintList.get(total_reader_constraints).length() - 2).equals(" =")){
                        readerConstraintList.remove(total_reader_constraints);
                        total_reader_constraints--;

                }	

                String lastConstraint = readerConstraintList.get(total_reader_constraints);
                if(lastConstraint.substring(lastConstraint.length() - 3).equals("and")){
                        String currentConstraint = readerConstraintList.get(total_reader_constraints);
                        currentConstraint = currentConstraint.substring(0, currentConstraint.length() - 4);
                        readerConstraintList.set(total_reader_constraints, currentConstraint);
                }
                
                logger.log(Level.INFO, "Reader constraint list: {0}", readerConstraintList);

                

                return readerConstraintList;
                        
        }


        public String makeWriterEntitiesCamflow(ArrayList<String> writerConstraintList, String templateName){

                try{
                        // executing readers constraints
                        String chainedWriterConstraint = "";
                        int total_writer_constraints = writerConstraintList.size();
                        for(int i = 0; i <= total_writer_constraints; i++){
                                queryClient.executeQuery(writerConstraintList.get(i));
                                chainedWriterConstraint += "%writer_constraint" + String.valueOf(i) + " or ";
                        }
                        if(chainedWriterConstraint.length() > 0){
                                chainedWriterConstraint = chainedWriterConstraint.substring(0, chainedWriterConstraint.length() - 4);
                        }

                        logger.log(Level.INFO, "Chained writer constraint: {0}", chainedWriterConstraint);

                        // getting reader entities
                        queryClient.executeQuery("$crossnamespace_writers"+ templateName +" = $base.getVertex(" + chainedWriterConstraint + ")");
                
                }catch(Exception e){
                        logger.log(Level.WARNING, "Error in querying", e);
                }

                return "$crossnamespace_writers" + templateName;

        }

        public ArrayList<String> getWriterConstraintList(HashSet<TreeMap<String, String>> completeOtherWriters){
                // --- writer constraint ---
                // getting all the writer ids
                
                int total_writer_constraints = 0;

                String writerConstraint = "%writer_constraint" + String.valueOf(total_writer_constraints) + " =";
                ArrayList<String> writerConstraintList = new ArrayList<String>();
                writerConstraintList.add(writerConstraint);

                int iter = 0;
                for(final TreeMap<String, String> writer : completeOtherWriters){
			String currentConstraint = writerConstraintList.get(total_writer_constraints);
                        currentConstraint += " \"id\" == '" + writer.get("id") + "' and";
                        writerConstraintList.set(total_writer_constraints, currentConstraint);

                        if((iter != 0) && (iter%6 == 0)){
                                currentConstraint = writerConstraintList.get(total_writer_constraints);
                                currentConstraint = currentConstraint.substring(0, currentConstraint.length() - 4);
                                writerConstraintList.set(total_writer_constraints, currentConstraint);

                                total_writer_constraints++;
				
                                writerConstraint = "%writer_constraint" + String.valueOf(total_writer_constraints) + " =";
                                writerConstraintList.add(writerConstraint);
                        }
                        iter++;
		}
		if(writerConstraintList.get(total_writer_constraints).substring(writerConstraintList.get(total_writer_constraints).length() - 2).equals(" =")){
			writerConstraintList.remove(total_writer_constraints);
			total_writer_constraints--;
		
		}

		logger.log(Level.INFO, "Writer constraint list: {0}", writerConstraintList);

                String lastConstraint = writerConstraintList.get(total_writer_constraints);
                if(lastConstraint.substring(lastConstraint.length() - 3).equals("and")){
                        String currentConstraint = writerConstraintList.get(total_writer_constraints);
                        currentConstraint = currentConstraint.substring(0, currentConstraint.length() - 4);
                        writerConstraintList.set(total_writer_constraints, currentConstraint);
                }
                
                return writerConstraintList;
        }

        public void remainingQueriesCamflow(){

                for (Map.Entry<String,ArrayList<String>> mapElement : ENTITY_TO_READER_WRITER.entrySet()){
                        String crossnamespaceEntityVariable = mapElement.getKey();
                        ArrayList<String> readerAndWriter = mapElement.getValue();

                        String crossnamespaceReaderVariable = readerAndWriter.get(0);
                        String crossnamespaceWriterVariable = readerAndWriter.get(1);

                        // Group all process memory vertices which contain the namespace identifiers.
                        queryClient.executeQuery("$memorys = $base.getVertex(object_type = 'process_memory')");
                        // Group all task vertices which contain the process identifiers. Tasks are connected to process memory vertices.
                        queryClient.executeQuery("$tasks = $base.getVertex(object_type = 'task')");
                        // Group all files vertices which represent an inode. Tasks are connected to files by 'relation_type'='read' or 'relation_type'='write'.
                        queryClient.executeQuery("$files = $base.getVertex(object_type = 'file')");
                        // Group all path vertices which contain the path of an inode in the filesystem. Files are connected to paths.
                        queryClient.executeQuery("$paths = $base.getVertex(object_type = 'path')");
                        // Group all argv vertices which contain the argument passed to a process.
                        queryClient.executeQuery("$argvs = $base.getVertex(object_type = 'argv')");


                        // Construct crossnamespace path
                        queryClient.executeQuery("$connected_entities = $base.getPath("+ crossnamespaceEntityVariable +", "+ crossnamespaceEntityVariable +", 1)");
                        queryClient.executeQuery("$crossnamespace_flow_0 = $base.getPath(" + crossnamespaceReaderVariable + ", " + crossnamespaceEntityVariable + ", 1)");
                        queryClient.executeQuery("$crossnamespace_flow_1 = $base.getPath(" + crossnamespaceEntityVariable + ", " + crossnamespaceWriterVariable + ", 1)");

                        queryClient.executeQuery("$crossnamespace_path_vertices = $base.getPath(" + crossnamespaceEntityVariable + ", $paths, 1) & $paths");
                        queryClient.executeQuery("$crossnamespace_path = $base.getPath($connected_entities, $crossnamespace_path_vertices, 1)");


                        // Adding process_memory vertices to writing and reading tasks.
                        queryClient.executeQuery("$writing_process_memory = $base.getLineage(" + crossnamespaceWriterVariable + ", 1, 'a') & $memorys");
                        queryClient.executeQuery("$reading_process_memory = $base.getLineage(" + crossnamespaceReaderVariable + ", 1, 'd') & $memorys");
                        queryClient.executeQuery("$writing_task_to_writing_memory = $base.getPath(" + crossnamespaceWriterVariable + ", $writing_process_memory, 1)");
                        queryClient.executeQuery("$reading_memory_to_reading_task = $base.getPath($reading_process_memory, " + crossnamespaceReaderVariable + ", 1)");

                        queryClient.executeQuery("$writing_process_memory_all_versions = $memorys.getMatch($writing_process_memory, 'object_id', 'cf:machine_id', 'boot_id')");
                        queryClient.executeQuery("$reading_process_memory_all_versions = $memorys.getMatch($reading_process_memory, 'object_id', 'cf:machine_id', 'boot_id')");

                        queryClient.executeQuery("$writing_process_memory_path = $base.getPath($writing_process_memory_all_versions, $writing_process_memory_all_versions, 1, $paths, 1)");
                        queryClient.executeQuery("$reading_process_memory_path = $base.getPath($reading_process_memory_all_versions, $reading_process_memory_all_versions, 1, $paths, 1)");

                        // Adding argv vertices to process_memory vertices.
                        
                        queryClient.executeQuery("$writing_process_to_argv = $base.getPath($writing_process_memory_all_versions, $argvs, 1)");
                        queryClient.executeQuery("$reading_process_to_argv = $base.getPath($reading_process_memory_all_versions, $argvs, 1)");

                        // Cross-namespace provenance subgraph construction.
                        queryClient.executeQuery("$subgraph = $crossnamespace_flow_0 + $crossnamespace_flow_1 + $connected_entities + $crossnamespace_path + $writing_task_to_writing_memory + $reading_memory_to_reading_task + $writing_process_memory_path + $reading_process_memory_path + $writing_process_to_argv + $reading_process_to_argv");
                        queryClient.executeQuery("$subgraph = $subgraph.collapseEdge('relation_type')");

                        queryClient.executeQuery("$transformed_subgraph = $subgraph.transform(MergeVertex,\"boot_id,cf:machine_id,object_id,pidns,ipcns,mntns,netns,cgroupns,utsns\")");
                        queryClient.executeQuery("$transformed_subgraph = $transformed_subgraph.collapseEdge('relation_type')");

                }

                
        }

}



