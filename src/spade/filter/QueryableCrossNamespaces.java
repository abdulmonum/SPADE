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

public class QueryableCrossNamespaces extends CrossNamespaces{

	private static final Logger logger = Logger.getLogger(QueryableCrossNamespaces.class.getName());

	private RemoteSPADEQueryConnection queryClient;
	
	@Override
	public boolean initialize(final String arguments){
		if(!super.initialize(arguments)){
			return false;
		}
		final String thisConfigFilePath = Settings.getDefaultConfigFilePath(QueryableCrossNamespaces.class.getClass());
		final String parentConfigFilePath = Settings.getDefaultConfigFilePath(CrossNamespaces.class.getClass());
		try{
			final Map<String, String> configMap = HelperFunctions.parseKeyValuePairsFrom(
					arguments, new String[]{thisConfigFilePath, parentConfigFilePath}
					);

			queryClient = new RemoteSPADEQueryConnection(Kernel.getHostName(), "localhost", Settings.getCommandLineQueryPort());
			queryClient.connect(Kernel.getClientSocketFactory(), 5 * 1000);
			// Set the storage appropriately i.e. based on the one that is being used
			queryClient.setStorage(PostgreSQL.class.getSimpleName());

			// Log any arguments that you get from 'configMap' TODO
			logger.log(Level.INFO, "Arguments: ");
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
			queryClient.executeQuery("");

			/*
			 * Export a graph
			 * 
			 * Do not export big graphs because of memory constraint
			 * 
			 * This is the same Graph class that is seen in Transformers
			 */
			final String symbol = "$small_graph_to_export"; // Name of the graph symbol to export
			final boolean force = true; // Export even if graph is big
			final boolean verify = false; // Do not verify the query response
			final spade.core.Graph graph = queryClient.exportGraph(symbol, force, verify);

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
