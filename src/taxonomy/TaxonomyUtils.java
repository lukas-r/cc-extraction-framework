package taxonomy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import db.DbConnection;
import db.PairType;
import utils.DbUtils;

public class TaxonomyUtils {
	
	private File taxonomyDbFile;
	
	public TaxonomyUtils(File taxonomyDbFile) {
		this.taxonomyDbFile = taxonomyDbFile;
	}
	
	public void exportEdgeList(File edgesFile, PairType pairType) throws SQLException, IOException {
		BufferedWriter output = new BufferedWriter(new FileWriter(edgesFile));
		
		DbConnection db = new DbConnection(this.taxonomyDbFile, true) {};
		int edgeCount = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT count(*) FROM " + pairType.name().toLowerCase() + "_edge")).getInt(1);
		ResultSet edges = db.connection.createStatement().executeQuery("SELECT child, parent FROM " + pairType.name().toLowerCase() + "_edge ORDER BY child, parent");
		int i = 0;
		while (edges.next()) {
			if (i % 1000000 == 0) {
				System.out.println(String.format("%.2f %%", 100.0D * i / edgeCount));
			}
			output.write(edges.getString(1) + " " + edges.getString(2));
			output.newLine();
			i++;
		}
		output.close();
		edges.close();
		db.close();
	}
	
	public void removeEdges(File removedEdges, PairType pairType) throws IOException, SQLException {
		DbConnection db = new DbConnection(this.taxonomyDbFile, false) {};
		db.connection.setAutoCommit(false);
		BufferedReader reader = new BufferedReader(new FileReader(removedEdges));
		PreparedStatement removeEdgeQuery = db.connection.prepareStatement("DELETE FROM " + pairType.name().toLowerCase() + "_edge WHERE child = ? AND parent = ?");
		String line;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(" ");
			removeEdgeQuery.setInt(1, Integer.parseInt(parts[0]));
			removeEdgeQuery.setInt(2, Integer.parseInt(parts[1]));
			removeEdgeQuery.execute();
		}
		reader.close();
		db.connection.commit();
		db.close();
	}
	
	public void exportNeo4JCsv(File crawlDbFile, File outputDir, PairType pairType) throws IOException, SQLException {
		File nodeFile = new File(outputDir.getAbsolutePath() + File.separator + pairType.name().toLowerCase() + "_nodes.csv");
		File edgeFile =  new File(outputDir.getAbsolutePath() + File.separator + pairType.name().toLowerCase() + "_edges.csv");
		
		DbConnection taxonomyDb = new DbConnection(this.taxonomyDbFile, true) {};
		DbConnection crawlDb = new DbConnection(crawlDbFile, true) {};

		int nodeCount = DbUtils.getFirstRow(taxonomyDb.connection.createStatement().executeQuery("SELECT count(*) FROM " + pairType.name().toLowerCase() + "_node")).getInt(1);
		BufferedWriter nodeWriter = new BufferedWriter(new FileWriter(nodeFile));
		nodeWriter.write("id:ID(" + pairType.name() + "),label,:LABEL");
		nodeWriter.newLine();
		PreparedStatement labelQuery = crawlDb.connection.prepareStatement(
			pairType == PairType.LEMMA ?
			"SELECT words FROM lemmagroup WHERE id = ?" :
			"SELECT pre.words || \" \" || ng.words || \" \" || post.words FROM term as t, premod as pre, noungroup as ng, postmod as post WHERE t.id = ? AND pre.id = t.premod AND ng.id = t.noungroup AND post.id = t.postmod"
		);
		ResultSet nodes = taxonomyDb.connection.createStatement().executeQuery("SELECT id, " + pairType.name().toLowerCase() + " AS token FROM " + pairType.name().toLowerCase() + "_node");
		int i = 0;
		while (nodes.next()) {
			if (i % 1000000 == 0) {
				System.out.println(String.format("nodes %.2f %%", 100.0D * i / nodeCount));
			}
			labelQuery.setInt(1, nodes.getInt(2));
			nodeWriter.write(nodes.getInt(1) + ",\"" + DbUtils.getFirstRow(labelQuery.executeQuery()).getString(1).trim().replace(";",  "").replace("\"", "\"\"") + "\"," + pairType.name());
			nodeWriter.newLine();
			i++;
		}
		nodeWriter.close();
		crawlDb.close();
		
		int edgeCount = DbUtils.getFirstRow(taxonomyDb.connection.createStatement().executeQuery("SELECT count(*) FROM " + pairType.name().toLowerCase() + "_edge")).getInt(1);
		BufferedWriter edgeWriter = new BufferedWriter(new FileWriter(edgeFile));
		edgeWriter.write(":START_ID(" + pairType.name() + "),:END_ID(" + pairType.name() + "),:TYPE");
		edgeWriter.newLine();
		ResultSet edges = taxonomyDb.connection.createStatement().executeQuery("SELECT child, parent FROM " + pairType.name().toLowerCase() + "_edge");
		i = 0;
		while (edges.next()) {
			if (i % 1000000 == 0) {
				System.out.println(String.format("edges %.2f %%", 100.0D * i / edgeCount));
			}
			edgeWriter.write(edges.getInt(1) + "," + edges.getInt(2) + ",IS_A");
			edgeWriter.newLine();
			i++;
		}
		edgeWriter.close();
		taxonomyDb.close();
	}
	
	public void createTransitiveClosure(PairType pairType) throws SQLException {
		DbConnection db = new DbConnection(this.taxonomyDbFile, false) {};
		db.connection.setAutoCommit(false);
		int nodeCount = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT count(*) FROM " + pairType.name().toLowerCase() + "node")).getInt(1);
		PreparedStatement connectionQuery = db.connection.prepareStatement("SELECT parent.parent, child.child, child.child_lemma FROM " + pairType.name().toLowerCase() + "_edge as child, " + pairType.name().toLowerCase() + "_edge as parent WHERE child.parent = ? AND parent.child = ?");
		PreparedStatement edgeInsertQuery = db.connection.prepareStatement("INSERT INTO " + pairType.name().toLowerCase() + "_edge (parent, child, child_lemma) VALUES (?, ?, ?)");
		ResultSet nodes = db.connection.createStatement().executeQuery("SELECT id FROM " + pairType.name().toLowerCase() + "_node");
		int i = 0;
		while (nodes.next()) {
			if (i % 1000000 == 0) {
				System.out.println(String.format("nodes %.2f %%", 100.0D * i / nodeCount));
			}
			i++;
			int nodeId = nodes.getInt(1);
			connectionQuery.setInt(1, nodeId);
			connectionQuery.setInt(2, nodeId);
			ResultSet connections = connectionQuery.executeQuery();
			while (connections.next()) {
				edgeInsertQuery.setInt(1, connections.getInt(1));
				edgeInsertQuery.setInt(2, connections.getInt(2));
				edgeInsertQuery.setInt(3, connections.getInt(3));
				edgeInsertQuery.execute();
			}
			db.connection.commit();
		}
		db.close();
	}

}
