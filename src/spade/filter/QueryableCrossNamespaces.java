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
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

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

                        queryClient.executeQuery("reset workspace");

                        // --- entity constraint ---
                        // extracting matched annotations and making their constraints
                        String graphName = "";
                        String entityConstraint = "";

                        for(Map.Entry<String,String> entry : matchedArtifactAnnotations.entrySet()) {
                                String key = entry.getKey();
                                String value = entry.getValue();

                                entityConstraint += " \"" + key + "\" == " + "'" + value + "'" + " and";
                                graphName += value + "_";
                        }
                        entityConstraint = entityConstraint.substring(0, entityConstraint.length() - 4);
                        graphName += "graph.dot";

                        queryClient.executeQuery("%entity_constraint =" + entityConstraint);

                        logger.log(Level.INFO, "Entity Constraint: {0}, Event id: {1}", new Object[]{entityConstraint, eventId});

                        // getting crossnamepace entities
                        queryClient.executeQuery("$crossnamespace_entities = $base.getVertex(%entity_constraint)");

                        /*
                         * Export a graph
                         *
                         * Do not export big graphs because of memory constraint
                         *
                         * This is the same Graph class that is seen in Transformers
                         */
                        final String symbol = "$crossnamespace_entities"; // Name of the graph symbol to export
                        final boolean force = true; // Export even if graph is big
                        final boolean verify = false; // Do not verify the query response
                        final spade.core.Graph graph = queryClient.exportGraph(symbol, force, verify);

                        // Exporting graph to a dot file
                        String completeFilePath = ABSOLUTE_EXPORT_PATH + graphName;
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
}
