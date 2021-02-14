package taxonomy;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.google.common.collect.Sets;

import db.DbConnection;
import db.PairType;
import utils.DbUtils;
import utils.measures.EnumTimer;

public class TaxonomyCreator extends DbConnection {
	
	public static final int SIMILARITY_THRESHOLD = 5;
	
	public static final int THREAD_NO_HORIZONTAL_MERGE = 3;
	public static final int THREAD_NO_VERTICAL_MERGE = 1;
	
	private File helperDbFile;
	private File tmpDbPath;
	
	private enum TimerMeasures {
		CREATE,
		MERGE_HORIZONTALLY,
		MH_LEMMA,
		MH_TERM,
		MH_QUERY_PARENTS,
		MH_READ_PARENTS,
		MH_MERGE_PARENTS,
		MH_WRITE,
		MHW_PARENTS,
		MHW_CHILDREN,
		MHW_EDGE,
		MERGE_VERTICALLY,
		MV_FIND_PARENTS,
		MV_WRITE_PARENTS,
		MV_FIND_UPPER_CHILDREN,
		MV_WRITE_UPPER_CHILDREN,
		MV_FIND_LOWER_PARENTS,
		MV_FIND_LOWER_CHILDREN,
		MV_WRITE_LOWER_CHILDREN,
		MV_COMPARE_CHILDREN,
		MV_CHECK_HAS_CHILDREN,
		MV_ADD_NODE,
		MV_CHANGE_NODE
	}
	
	private EnumTimer<TimerMeasures> timer;
	
	public TaxonomyCreator(File db, File helperDbFile, File tmpDbPath) throws IOException {
		super(db, false);
		
		this.helperDbFile = helperDbFile;
		this.tmpDbPath = tmpDbPath;
		
		this.timer = new EnumTimer<>(TimerMeasures.class);
	}
	
	public void create() throws IOException, SQLException, InterruptedException, ExecutionException {
		this.create(PairType.LEMMA);
		this.create(PairType.TERM);
	}
	
	public void create(PairType pairType) throws IOException, SQLException, InterruptedException, ExecutionException {
		this.timer.start(TimerMeasures.CREATE);
		this.timer.start(TimerMeasures.MERGE_HORIZONTALLY);
		this.mergeHorizontally(pairType);
		this.timer.stop(TimerMeasures.MERGE_HORIZONTALLY);
		this.connection.commit();
		this.timer.start(TimerMeasures.MERGE_VERTICALLY);
		this.mergeVertically(pairType);
		this.timer.start(TimerMeasures.MERGE_VERTICALLY);
		this.connection.commit();
		this.timer.stop(TimerMeasures.CREATE);
	}
	
	private static boolean areSimilar(Set<Integer> s1, Set<Integer> s2) {
		return Sets.intersection(s1, s2).size() >= Collections.min(Arrays.asList(s1.size(), s2.size(), Math.max(2, (int) (Math.min(s1.size(), s2.size()) * 0.5) + 1), SIMILARITY_THRESHOLD));
	}
	
	private List<Set<Integer>> getMergedParents (DbConnection helperDb, int tokenId, PairType pairType, AtomicInteger mergeCount) throws SQLException, InterruptedException {
		this.timer.start(TimerMeasures.MH_QUERY_PARENTS);
		int parentCount = DbUtils.getFirstRow(helperDb.connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + pairType.name().toLowerCase() + "_node WHERE " + pairType.name().toLowerCase() + " = " + tokenId + " AND parent = -1")).getInt(1);
		ResultSet parents = helperDb.connection.createStatement().executeQuery("SELECT children_" + pairType.name().toLowerCase() + "s FROM " + pairType.name().toLowerCase() + "_node WHERE " + pairType.name().toLowerCase() + " = " + tokenId + " AND parent = -1");
		this.timer.stop(TimerMeasures.MH_QUERY_PARENTS);
		List<Set<Integer>> mergedParents = new ArrayList<Set<Integer>>(parentCount);
		
		this.timer.start(TimerMeasures.MH_READ_PARENTS);
		for (int i = 0; i < parentCount; i++) {
			parents.next();
			mergedParents.add(new HashSet<Integer>(Stream.of(parents.getString(1).split(";;")).map(s -> s.replace(";", "")).map(s -> Integer.parseInt(s)).collect(Collectors.toList())));
		}
		this.timer.stop(TimerMeasures.MH_READ_PARENTS);
		parents.close();
		
		mergedParents.sort((s1, s2) -> s2.size() - s1.size());
		
		this.timer.start(TimerMeasures.MH_MERGE_PARENTS);
		boolean changed = mergedParents.get(0).size() > 0;
		boolean singlesExcluded = false;
		while (changed) {
			changed = false;
			for (int p1 = 0; p1 < mergedParents.size() - 1; p1++) {
				if (mergedParents.get(p1).size() == 1) {
					break;
				}
				for (int p2 = p1 + 1; p2 < mergedParents.size(); p2++) {
					if (singlesExcluded && mergedParents.get(p2).size() == 1) {
						break;
					}
					if (areSimilar(mergedParents.get(p1), mergedParents.get(p2))) {
						mergeCount.addAndGet(1);
						mergedParents.get(p1).addAll(mergedParents.get(p2));
						mergedParents.remove(p2);
						p2--;
						changed = true;
					}
				}
			}
			singlesExcluded = true;
		}
		this.timer.stop(TimerMeasures.MH_MERGE_PARENTS);
		return mergedParents;
	}

	private void mergeHorizontally(PairType pairType) throws SQLException, InterruptedException, ExecutionException {
		PreparedStatement insertNodeQuery = this.connection.prepareStatement("INSERT INTO " + pairType.name().toLowerCase() + "_node (" + pairType.name().toLowerCase() + ") VALUES (?)");
		PreparedStatement insertEdgeQuery = this.connection.prepareStatement("INSERT INTO " + pairType.name().toLowerCase() + "_edge (parent, child, child_token) VALUES (?, ?, ?)");
		
		DbConnection helperDb = new DbConnection(this.helperDbFile, true) {};
		int tokenCount = DbUtils.getFirstRow(helperDb.connection.createStatement().executeQuery("SELECT COUNT(DISTINCT " + pairType.name().toLowerCase() + ") FROM " + pairType.name().toLowerCase() + "_node WHERE parent = -1")).getInt(1);
		ResultSet tokens = helperDb.connection.createStatement().executeQuery("SELECT DISTINCT " + pairType.name().toLowerCase() + " FROM " + pairType.name().toLowerCase() + "_node WHERE parent = -1");
		
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_NO_HORIZONTAL_MERGE);
		CompletionService<DbConnection> completionService = new ExecutorCompletionService<DbConnection>(executor);
		
		AtomicInteger mergeCount = new AtomicInteger();
		
		BiFunction<Integer, DbConnection, Runnable> runnableFactory = (tokenId, db) -> {
			return () -> {
				try {
					EnumTimer<TimerMeasures> timer = new EnumTimer<>(TimerMeasures.class);
					List<Set<Integer>> mergedParents = getMergedParents(db, tokenId, pairType, mergeCount);
					
					timer.start(TimerMeasures.MH_WRITE);
					for (Set<Integer> children: mergedParents) {
						int parentNodeId;
						timer.start(TimerMeasures.MHW_PARENTS);
						synchronized (insertNodeQuery) {							
							insertNodeQuery.setInt(1, tokenId);
							insertNodeQuery.execute();
							parentNodeId = DbUtils.getInsertedKey(insertNodeQuery);
						}
						timer.stop(TimerMeasures.MHW_PARENTS);
						
						for (int child: children) {
							int childNodeId;
							timer.start(TimerMeasures.MHW_CHILDREN);
							synchronized (insertNodeQuery) {
								insertNodeQuery.setInt(1, child);
								insertNodeQuery.execute();
								childNodeId = DbUtils.getInsertedKey(insertNodeQuery);
							}
							timer.stop(TimerMeasures.MHW_CHILDREN);
							
							timer.start(TimerMeasures.MHW_EDGE);
							synchronized (insertNodeQuery) {								
								insertEdgeQuery.setInt(1, parentNodeId);
								insertEdgeQuery.setInt(2, childNodeId);
								insertEdgeQuery.setInt(3, child);
								insertEdgeQuery.execute();
							}
							timer.stop(TimerMeasures.MHW_EDGE);
						}
					}
					timer.stop(TimerMeasures.MH_WRITE);
					TaxonomyCreator.this.timer.incorporate(timer);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
			};
		};
		
		for (int i = 0; i < THREAD_NO_HORIZONTAL_MERGE; i++) {
			tokens.next();
			int tokenId = tokens.getInt(1);
			DbConnection connection = new DbConnection(this.helperDbFile, true) {};
			completionService.submit(runnableFactory.apply(tokenId, connection), connection);
		}
		
		int pi = 0;
		while (tokens.next()) {
			int tokenId = tokens.getInt(1);
			
			if (pi % 1000 == 0) {				
				System.out.println("merge horizontally " + tokenId + " " + pairType +  " " + pi + String.format(" %.2f %%", 100.0D * pi / tokenCount) + " merge count " +  mergeCount);
				System.out.println(this.timer);
			}
			pi++;
			
			DbConnection connection = completionService.take().get();
			if (pi / THREAD_NO_HORIZONTAL_MERGE % 2500 == 0) {
				connection.close();
				connection = new DbConnection(this.helperDbFile, true) {};
			}
			if (pi % 100000 == 0) {
				System.out.println("COMMIT");
				this.connection.commit();
			}
			completionService.submit(runnableFactory.apply(tokenId, connection), connection);
		}
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		insertNodeQuery.close();
		insertEdgeQuery.close();
		helperDb.close();
	}
	
	private void mergeVertically(PairType pairType) throws SQLException, InterruptedException, IOException {
		File tmpDbFile = File.createTempFile("tmp_db_", ".sqlite", tmpDbPath);
		DbConnection tmpDb = new DbConnection(tmpDbFile) {
			@Override
			protected void prepare() {
				this.context.createTable(pairType.name().toLowerCase() + "_parent").column(DSL.field("node_id")).execute();
			}
		};
		int parentCount = 0;
		
		System.out.println("WRITE PARENTS");
		PreparedStatement insertParentQuery = tmpDb.connection.prepareStatement("INSERT INTO " + pairType.name().toLowerCase() + "_parent (node_id) VALUES (?)");
		this.timer.start(TimerMeasures.MV_FIND_PARENTS);
		ResultSet parents = this.connection.createStatement().executeQuery("SELECT DISTINCT parent FROM " + pairType.name().toLowerCase() + "_edge");
		this.timer.stop(TimerMeasures.MV_FIND_PARENTS);
		this.timer.start(TimerMeasures.MV_WRITE_PARENTS);
		while (parents.next()) {
			if (parentCount % 100000 == 0) {
				System.out.println("WRITE PARENT " + parentCount);
			}
			insertParentQuery.setInt(1, parents.getInt(1));
			insertParentQuery.execute();
			parentCount++;
		}
		insertParentQuery.close();
		this.timer.stop(TimerMeasures.MV_WRITE_PARENTS);
		System.out.println("WRITE PARENTS DONE");
		
		PreparedStatement getParentsQuery = tmpDb.connection.prepareStatement("SELECT node_id FROM " + pairType.name().toLowerCase() + "_parent");
		PreparedStatement getChildrenQuery = this.connection.prepareStatement("SELECT child, child_token, id FROM " + pairType.name().toLowerCase() + "_edge WHERE parent = ?");
		PreparedStatement getParentsByLemmaQuery = this.connection.prepareStatement("SELECT id FROM " + pairType.name().toLowerCase() + "_node WHERE " + pairType.name().toLowerCase() + " = ?");
		PreparedStatement updateEdgeChildQuery = this.connection.prepareStatement("UPDATE " + pairType.name().toLowerCase() + "_edge SET child = ? WHERE id = ?");
		PreparedStatement insertEdgeQuery = this.connection.prepareStatement("INSERT INTO " + pairType.name().toLowerCase() + "_edge (parent, child, child_token) VALUES (?, ?, ?)");
		PreparedStatement deleteNodeQuery = this.connection.prepareStatement("DELETE FROM " + pairType.name().toLowerCase() + "_node WHERE id = ?");
		PreparedStatement hasChildrenQuery = this.connection.prepareStatement("SELECT id FROM " + pairType.name().toLowerCase() + "_edge WHERE parent = ? LIMIT 1");
		
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_NO_VERTICAL_MERGE);
		CompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
		
		AtomicInteger addCount = new AtomicInteger();
		AtomicInteger changeCount = new AtomicInteger();
		
		Function<Integer, Runnable> runnableFactory = (upperParentId) -> {
			return () -> {
				try {
					EnumTimer<TimerMeasures> timer = new EnumTimer<>(TimerMeasures.class);
					Map<Integer, Pair<Integer, Integer>> upperChildren = new HashMap<Integer, Pair<Integer, Integer>>();
					ResultSet upperChildrenSet;
					timer.start(TimerMeasures.MV_FIND_UPPER_CHILDREN);
					synchronized (getChildrenQuery) {
						getChildrenQuery.setInt(1, upperParentId);
						upperChildrenSet = getChildrenQuery.executeQuery();
					}
					timer.stop(TimerMeasures.MV_FIND_UPPER_CHILDREN);
					timer.start(TimerMeasures.MV_WRITE_UPPER_CHILDREN);
					while (upperChildrenSet.next()) {
						upperChildren.put(upperChildrenSet.getInt(2), Pair.of(upperChildrenSet.getInt(1), upperChildrenSet.getInt(3)));
					}
					if (upperChildren.size() == 1) {
						return;
					}
					timer.stop(TimerMeasures.MV_WRITE_UPPER_CHILDREN);
					for (Entry<Integer, Pair<Integer, Integer>> upperChild: upperChildren.entrySet()) {
						int upperChildId = upperChild.getValue().getLeft();
						int upperEdgeId = upperChild.getValue().getRight();
						int upperChildTokenId = upperChild.getKey();
						
						ResultSet lowerParents;
						timer.start(TimerMeasures.MV_FIND_LOWER_PARENTS);
						synchronized (getParentsByLemmaQuery) {
							getParentsByLemmaQuery.setInt(1, upperChildTokenId);							
							lowerParents = getParentsByLemmaQuery.executeQuery();
						}
						timer.stop(TimerMeasures.MV_FIND_LOWER_PARENTS);
						while (lowerParents.next()) {
							int lowerParentId = lowerParents.getInt(1);
							if (lowerParentId == upperParentId) {
								continue;
							}
							Set<Integer> lowerChildren = new HashSet<Integer>();
							ResultSet lowerChildrenSet;
							timer.start(TimerMeasures.MV_FIND_LOWER_CHILDREN);
							synchronized (getChildrenQuery) {								
								getChildrenQuery.setInt(1, lowerParentId);
								lowerChildrenSet = getChildrenQuery.executeQuery();
							}
							timer.stop(TimerMeasures.MV_FIND_LOWER_CHILDREN);
							if (!lowerChildrenSet.isBeforeFirst()) {
								continue;
							}
							timer.start(TimerMeasures.MV_WRITE_LOWER_CHILDREN);
							while (lowerChildrenSet.next()) {
								lowerChildren.add(lowerChildrenSet.getInt(1));
							}
							timer.stop(TimerMeasures.MV_WRITE_LOWER_CHILDREN);
							if (areSimilar(Sets.difference(upperChildren.keySet(), new HashSet<Integer>(Arrays.asList(upperChildTokenId))), Sets.difference(lowerChildren, new HashSet<Integer>(Arrays.asList(upperChildTokenId))))) {
								boolean hasChildren;
								timer.start(TimerMeasures.MV_CHECK_HAS_CHILDREN);
								synchronized (hasChildrenQuery) {									
									hasChildrenQuery.setInt(1, upperChildId);
									hasChildren = hasChildrenQuery.executeQuery().isBeforeFirst();
								}
								timer.stop(TimerMeasures.MV_CHECK_HAS_CHILDREN);
								if (hasChildren) {
									timer.start(TimerMeasures.MV_ADD_NODE);
									synchronized (insertEdgeQuery) {										
										insertEdgeQuery.setInt(1, upperParentId);
										insertEdgeQuery.setInt(2, lowerParentId);
										insertEdgeQuery.setInt(3, upperChildTokenId);
										try {
											insertEdgeQuery.execute();											
										} catch (Exception e) {
											e.printStackTrace();
											System.out.println(upperParentId);
											System.out.println(upperChildId);
											System.out.println(upperChildTokenId);
											System.out.println(lowerParentId);
											System.exit(0);
										}
									}
									timer.stop(TimerMeasures.MV_ADD_NODE);
									addCount.addAndGet(1);
								} else {
									timer.start(TimerMeasures.MV_CHANGE_NODE);
									synchronized (updateEdgeChildQuery) {										
										updateEdgeChildQuery.setInt(1, lowerParentId);
										updateEdgeChildQuery.setInt(2, upperEdgeId);
										updateEdgeChildQuery.execute();
										deleteNodeQuery.setInt(1, upperChildId);
										deleteNodeQuery.execute();
									}
									timer.stop(TimerMeasures.MV_CHANGE_NODE);
									changeCount.addAndGet(1);
								}
							}
						}
					}
					TaxonomyCreator.this.timer.incorporate(timer);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
			};
		};
		
		ResultSet upperParent = getParentsQuery.executeQuery();
		
		for (int i = 0; i < THREAD_NO_VERTICAL_MERGE; i++) {
			upperParent.next();
			int tokenId = upperParent.getInt(1);
			completionService.submit(runnableFactory.apply(tokenId), null);
		}
		
		int i = 0;
		while (upperParent.next()) {
			int upperParentId = upperParent.getInt(1);
			if (i % 1000 == 0) {				
				System.out.println("merge vertically upper " + upperParentId + " " + pairType +  " " + i + " " + String.format(" %.2f %%", 100.0D * i / parentCount) + " added: " + addCount + " changed: " + changeCount);
			}
			
			if (i % 10000 == 0) {
				try {					
					System.out.println(timer);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			if (i % 100000 == 0) {
				System.out.println("COMMIT");
				this.connection.commit();
			}
			
			completionService.take();
			completionService.submit(runnableFactory.apply(upperParentId), null);
			i++;
		}
		
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		
		getParentsQuery.close();
		getChildrenQuery.close();
		getParentsByLemmaQuery.close();
		updateEdgeChildQuery.close();
		insertEdgeQuery.close();
		deleteNodeQuery.close();
		hasChildrenQuery.close();
		tmpDb.close();
		tmpDbFile.delete();
	}
	
	@Override
	protected void prepare() {
		try {
			this.connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void createTables() {
		this.context.createTable("lemma_node")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("lemma", SQLDataType.INTEGER)
			.execute();
		this.context.createIndex().on("lemma_node", "lemma").execute();
		
		this.context.createTable("lemma_edge")//to instance/child
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("parent", SQLDataType.INTEGER)
			.column("child", SQLDataType.INTEGER)
			.column("child_token", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("parent", "child"),
				DSL.foreignKey("parent").references("lemma_node"),
				DSL.foreignKey("child").references("lemma_node")
			)
			.execute();
		this.context.createIndex().on("lemma_edge", "parent").execute();
		this.context.createIndex().on("lemma_edge", "child").execute();
		
		this.context.createTable("term_node")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("term", SQLDataType.INTEGER)
			.execute();
		this.context.createIndex().on("term_node", "term").execute();
		
		this.context.createTable("term_edge")//to instance/child
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("parent", SQLDataType.INTEGER)
			.column("child", SQLDataType.INTEGER)
			.column("child_token", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("parent", "child"),
				DSL.foreignKey("parent").references("term_node"),
				DSL.foreignKey("child").references("term_node")
			)
			.execute();
		this.context.createIndex().on("term_edge", "parent").execute();
		this.context.createIndex().on("term_edge", "child").execute();
	}

}
