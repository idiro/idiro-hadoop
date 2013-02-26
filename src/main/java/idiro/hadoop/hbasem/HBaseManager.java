package idiro.hadoop.hbasem;

import idiro.check.DbChecker;
import idiro.utils.db.JdbcConnection;
import idiro.utils.tree.Tree;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * Class to manage the hbase metastore storing in a mysql database
 * @author etienne
 *
 * The HBaseManager can manage several hbase metastore. The class is not
 * done to manage all the hbase tables but only a part of them. It allows
 * separation for example between customers/working group 1/2/3. These groups
 * will be completely independent and no interaction can occur between them within
 * the programme.
 */
public class HBaseManager {

	/** Logger of the class */
	private Logger logger = Logger.getLogger(getClass());

	/** JDBC Connection, connect to the metastore database (mysql) */
	private JdbcConnection conn;

	/**
	 * Log the error message for failing in a query process and returns false
	 * @param e
	 * @return
	 */
	private boolean logErrorQuery(SQLException e){
		logger.error("Fail to execute a query");
		logger.error(e.getMessage());
		return false;
	}

	/**
	 * Constructor needs the name of the working group
	 * 
	 * @param nameToManage
	 * @throws Exception
	 */
	public HBaseManager(String nameToManage) throws Exception{
		HBaseDatabase db = new HBaseDatabase();
		conn = db.get(nameToManage); 
	}

	/**
	 * Add features to a table.
	 * 
	 * Methods to announce a new feature is stored in a HBase table.
	 * WARNING: The method does not check if the hbaseId is contained
	 * or not in the dictionary table
	 * 
	 * @param tableName name of the hbase table
	 * @param hbaseIdWithColFam key: hbase id, value: colomn family.
	 * @return
	 */
	public boolean addFeatureToTable(String tableName,Map<String,String>hbaseIdWithColFam){

		boolean ok = true;
		tableName = tableName.toLowerCase();
		if(tableName.equals(HBaseDatabase.dictionary_table) || tableName.equals(HBaseDatabase.category_table)){
			logger.error(tableName+" is an administration table (reserved)");
			return false;
		}
		tableName = tableName.toLowerCase();
		DbChecker dbCh = new DbChecker(conn);
		if(!dbCh.isTableExist(tableName)){
			Map<String,String> features = new LinkedHashMap<String,String>();
			features.put(HBaseDatabase.table_f_hbid,"VARCHAR(15) BINARY UNIQUE NOT NULL");
			features.put(HBaseDatabase.table_f_colfam, "VARCHAR(8) BINARY NOT NULL");
			String[] options = {HBaseDatabase.table_f_hbid};
			try {
				conn.createTable(tableName, features, options);
			} catch (SQLException e) {
				ok = logErrorQuery(e);
			}
		}
		if(ok){
			Iterator<String> it = hbaseIdWithColFam.keySet().iterator();
			try{
				while(it.hasNext()){
					String id = it.next();
					conn.execute("INSERT INTO "+tableName+" VALUES('"+
							id+"','"+hbaseIdWithColFam.get(id)+"')");
				}
			}catch(SQLException e){
				ok = logErrorQuery(e);
			}
		}
		if(!ok){
			logger.warn("Fail to add feature to the table '"+tableName+"'");
		}
		return ok;
	}



	/**
	 * Methods that announce the deletion of a feature to the metastore
	 * 
	 * You can delete either by the column family (remove all the element within
	 * the column family) or by the hbase id (remove one or zero element). You can
	 * also do both
	 * 
	 * @param tableName
	 * @param col_fam
	 * @param hbaseId
	 * @return
	 */
	public boolean deleteFeatureToTable(String tableName,String col_fam, String hbaseId){

		boolean ok = true;
		int i = 0;
		tableName = tableName.toLowerCase();
		if(tableName.equals(HBaseDatabase.dictionary_table) || 
				tableName.equals(HBaseDatabase.category_table)){
			logger.error(tableName+" is an administration table (reserved)");
			return false;
		}
		tableName = tableName.toLowerCase();
		if(col_fam != null && !col_fam.isEmpty()){
			++i;
		}
		if( hbaseId != null && !hbaseId.isEmpty()){
			++i;
		}
		if(i == 0){
			logger.warn("Nothing to delete, args are null or empty");
			return false;
		}

		DbChecker dbCh = new DbChecker(conn);
		if(!dbCh.isTableExist(tableName)){
			logger.warn("The table '"+tableName+"' does not exists, cannot remove any features");
			ok = false;
		}
		if(ok){
			String statement = "DELETE FROM "+tableName +" WHERE ";
			if(col_fam != null && !col_fam.isEmpty()){
				statement += HBaseDatabase.table_f_colfam+" = '"+col_fam+"'";
			}
			if(i == 2){
				statement += " OR ";
			}

			if( hbaseId != null && !hbaseId.isEmpty()){
				statement += HBaseDatabase.table_f_hbid+" = '"+hbaseId+"'";
			}
			try{
				conn.execute(statement);
			}catch(SQLException e){
				ok = logErrorQuery(e);
			}
		}
		return ok;
	}

	/**
	 * Add a feature to the dictionary.
	 * 
	 * The feature has to be discribed on one line with a delimiter
	 * The delimiter has to be a '|' or a non special regex charactere
	 * The line has to contains 4 or 5 columns (without or with the comment)
	 * 
	 * @param line
	 * @param delimiter
	 * @return
	 */
	public boolean addFeature(String line,char delimiter){
		String delStr = ""+delimiter;
		if(delimiter == '|'){
			delStr = "\\|";
		}
		String[] lineSplit = line.split(delStr);
		String comment = null;

		if(lineSplit.length == 5){
			comment = lineSplit[4];
		}else if(lineSplit.length != 4){
			logger.error("line split has to be of size 4 or 5 for matching table");
			logger.error("char : '"+delimiter+"' line: '"+line+"'");
			return false;
		}
		return addFeature(lineSplit[0],lineSplit[1],lineSplit[2],lineSplit[3],comment);
	}


	/**
	 * Add a feature to the dictionary.
	 * 
	 * @param feature
	 * @param hbaseId
	 * @param type
	 * @param category
	 * @param comment
	 * @return
	 */
	protected boolean addFeature(String feature, String hbaseId, String type, String category, String comment){
		boolean ok = true;
		try {
			conn.execute("INSERT INTO "+HBaseDatabase.dictionary_table+" VALUES('"+
					feature+"','"+hbaseId+"','"+type.toLowerCase()+"','"+category+"','"+comment+"')");

			if(!catExists(category)){
				conn.execute("INSERT INTO "+HBaseDatabase.category_table+" VALUES('"+
						category+"','unknown')");
			}
		} catch (SQLException e) {
			ok = logErrorQuery(e);
		}
		return ok;
	}

	/**
	 * Add a category to the category table.
	 * 
	 * The category has to be discribed on one line with a delimiter
	 * The delimiter has to be a '|' or a non special regex charactere
	 * The line has to contains 2 columns (category name, parent's name)
	 * 
	 * @param line
	 * @param delimiter
	 * @return
	 */
	public boolean addCategory(String line, char delimiter){
		String delStr = ""+delimiter;
		if(delimiter == '|'){
			delStr = "\\|";
		}
		String[] lineSplit = line.split(delStr);
		if(lineSplit.length != 2){
			logger.error("line split has to be of 2 for matching table");
			logger.error("char : '"+delimiter+"' line: '"+line+"'");
			return false;
		}
		return addCategory(lineSplit[0],lineSplit[1]);
	}

	/**
	 * Add a category to the category table.
	 * @param cat
	 * @param parentCat
	 * @return
	 */
	public boolean addCategory(String cat,String parentCat){
		boolean ok = true;
		try{
			conn.execute("INSERT INTO "+HBaseDatabase.category_table+" VALUES('"+
					cat+"','"+parentCat+"')");
		} catch (SQLException e) {
			ok = logErrorQuery(e);
		}
		return ok;
	}

	/**
	 * Check if a category exists
	 * @param category
	 * @return
	 */
	public boolean catExists(String category) {
		boolean ok = false;
		try {
			ResultSet resultSet = conn.executeQuery("select * from "+
					HBaseDatabase.category_table+" where CATEGORY = '"+category+"'");	
			while(resultSet.next()){
				ok = true;
			}
		} catch (SQLException e) {
			logErrorQuery(e);
		}
		return ok;
	}

	/**
	 * Check if a feature exists in the dictionary table
	 * @param feature
	 * @return
	 */
	public boolean featExists(String feature) {
		boolean ok = false;
		try {
			ResultSet resultSet = conn.executeQuery("select * from "+
					HBaseDatabase.dictionary_table+" where NAME = '"+feature+"'");	
			while(resultSet.next()){
				ok = true;
			}
		} catch (SQLException e) {
			logErrorQuery(e);
		}
		return ok;
	}

	/**
	 * Get the feature description.
	 * Index:
	 * 0: name
	 * 1: hbase_id
	 * 2: type
	 * 3: category
	 * 4: comment
	 * @param feature feature name
	 * @return
	 */
	public List<String> getFeatureDesc(String feature){
		List<String> ans = new LinkedList<String>();
		if(featExists(feature)){
			try {
				ResultSet resultSet = conn.executeQuery("select * from "+HBaseDatabase.dictionary_table+" where "+
						HBaseDatabase.dictionary_f_name+" = '"+feature+"'");	
				if(resultSet.next()){
					ans.add(resultSet.getString(HBaseDatabase.dictionary_f_name));
					ans.add(resultSet.getString(HBaseDatabase.dictionary_f_hbid));
					ans.add(resultSet.getString(HBaseDatabase.dictionary_f_type));
					ans.add(resultSet.getString(HBaseDatabase.dictionary_f_category));
					ans.add(resultSet.getString(HBaseDatabase.dictionary_f_comment));
				}
			} catch (SQLException e) {
				logErrorQuery(e);
			}
		}
		return ans;
	}

	/**
	 * Get all the HBase id used in a table
	 * @param table
	 * @return
	 */
	public List<String> getAllHBaseIdOf(String table){
		DbChecker dbCh = new DbChecker(conn);
		List<String> ans = new LinkedList<String>();
		if(dbCh.isTableExist(table)){
			try {
				ResultSet resultSet = conn.executeQuery("select * from "+table);	
				while(resultSet.next()){
					ans.add(resultSet.getString(HBaseDatabase.table_f_hbid));
				}
			} catch (SQLException e) {
				logErrorQuery(e);
			}
		}
		return ans;
	}


	/**
	 * Get a list of all the hbase tables
	 * @return
	 */
	public List<String> getTables(){
		List<String> ans = null;
		try {
			ans = conn.listTables(null);
			ans.remove(HBaseDatabase.category_table);
			ans.remove(HBaseDatabase.dictionary_table);
		} catch (SQLException e) {
			logger.error("Fail to list all the table with the hbase metastore");
		}
		return ans;
	}

	/**
	 * Get the hbase id and the column family of all the element of a table
	 * @param table
	 * @return
	 */
	public Map<String,String> getTable(String table){
		DbChecker dbCh = new DbChecker(conn);
		Map<String,String> ans = new LinkedHashMap<String,String>();
		if(dbCh.isTableExist(table)){
			try {
				ResultSet resultSet = conn.executeQuery("select * from "+table);	
				while(resultSet.next()){
					ans.put(resultSet.getString(HBaseDatabase.table_f_hbid),
							resultSet.getString(HBaseDatabase.table_f_colfam));
				}
			} catch (SQLException e) {
				logErrorQuery(e);
			}
		}
		return ans;
	}

	/**
	 * Get all the features contained in the dictionary
	 * @return
	 */
	public List<String> getAllFeatures(){
		List<String> ans = new LinkedList<String>();
		try {
			ResultSet resultSet = conn.executeQuery("select "+
					HBaseDatabase.dictionary_f_name+
					" from "+HBaseDatabase.dictionary_table);	
			while(resultSet.next()){
				ans.add(resultSet.getString(HBaseDatabase.dictionary_f_name));
			}
		} catch (SQLException e) {
			logErrorQuery(e);
		}
		return ans;
	}

	/**
	 * Get all the HBase id contained in the dictionary
	 * @return
	 */
	public List<String> getAllHbaseId(){
		List<String> ans = new LinkedList<String>();
		try {
			ResultSet resultSet = conn.executeQuery("select "+
					HBaseDatabase.dictionary_f_hbid+" from "+HBaseDatabase.dictionary_table);	
			while(resultSet.next()){
				ans.add(resultSet.getString(HBaseDatabase.dictionary_f_hbid));
			}
		} catch (SQLException e) {
			logErrorQuery(e);
		}
		return ans;
	}

	/**
	 * Get all the categories contained in the category table under the form of a tree
	 * @return
	 */
	public Tree<String> getCategories(){
		List<Tree<String>> listNotAttachedYet = new LinkedList<Tree<String>>();
		listNotAttachedYet.add(new Tree<String>("root"));
		List<Tree<String>> tree_parent = null,
				tree_children = null;
		Tree<String> cur;
		boolean found_parent,found_children;
		try {
			ResultSet resultSet = conn.executeQuery("select * from "+HBaseDatabase.category_table);	
			while(resultSet.next()){
				String child =  resultSet.getString(HBaseDatabase.category_f_cat);
				String parent = resultSet.getString(HBaseDatabase.category_f_parent);
				if(parent == null){
					listNotAttachedYet.get(0).add(new Tree<String>(child));
				}else{
					//Parent and Child search
					found_parent = found_children = false;
					Iterator<Tree<String>> it = listNotAttachedYet.iterator();
					while(it.hasNext() &&!found_parent && !found_children){
						cur = it.next();
						if(!found_parent){
							tree_parent = cur.findInTree(parent);
							if(!tree_parent.isEmpty()){
								if(tree_parent.size() > 1){
									logger.error("Two elements");
									return null;
								}
								found_parent = true;
							}
						}
						if(!found_children){
							tree_children = cur.findInTree(child);
							if(!tree_children.isEmpty()){
								if(tree_children.size() > 1){
									logger.error("Two elements");
									return null;
								}
								found_children = true;
							}
						}
					}
					if(found_parent){
						if(found_children){
							tree_parent.get(0).add(tree_children.get(0));
							listNotAttachedYet.remove(tree_children.get(0));
						}else{
							tree_parent.get(0).add(new Tree<String>(child));
						}
					}else{
						if(found_children){
							Tree<String> treeParent = new Tree<String>(parent);
							treeParent.add(tree_children.get(0));
							listNotAttachedYet.add(treeParent);
							listNotAttachedYet.remove(tree_children.get(0));
						}else{
							Tree<String> treeParent = new Tree<String>(parent);
							treeParent.add(new Tree<String>(child));
							listNotAttachedYet.add(treeParent);
						}
					}
				}
			}
		} catch (SQLException e) {
			logErrorQuery(e);
		}

		if(listNotAttachedYet.size() > 1){
			logger.error("Size of this list should be one");
		}

		return listNotAttachedYet.get(0);
	}

	/**
	 * Get a Map < hbaseId, feature Name > from a list of feature.
	 * 
	 * If the hbaseId is null or empty it returns all the features
	 * 
	 * @param hbaseId List of feature that we want to know the feature name;
	 * 
	 * @return
	 */
	public Map<String, String> getFeaturesFromId(Collection<String> hbaseId){
		Map<String,String> idWFeatures = new LinkedHashMap<String,String>();

		if(hbaseId == null || hbaseId.isEmpty()){
			hbaseId = getAllHbaseId();
		}

		try {
			ResultSet resultSet = conn.executeQuery("select "+
					HBaseDatabase.dictionary_f_name+", "+ HBaseDatabase.dictionary_f_hbid +" from "+HBaseDatabase.dictionary_table);	
			while(resultSet.next()){
				String curId = resultSet.getString(HBaseDatabase.dictionary_f_hbid);
				if(hbaseId.contains(curId)){
					idWFeatures.put(curId, resultSet.getString(HBaseDatabase.dictionary_f_name));
				}
			}
		}catch (SQLException e) {
			logErrorQuery(e);
			idWFeatures = new LinkedHashMap<String,String>();
		}
		return idWFeatures;
	}

	/**
	 * Get the hbase id of the corresponding features:
	 * 
	 * @param hiveFeatures hive feature with Key: feature name, value: type
	 * @return key: hive feature value hbase feature
	 */
	public Map<String, String> getMapFeatures(Map<String, String> hiveFeatures) {

		Map<String,String> ans = new LinkedHashMap<String,String>();
		Iterator<String> it = hiveFeatures.keySet().iterator();
		List<String> featDesc;
		while(it.hasNext()){
			String hiveFeat = it.next();
			featDesc = getFeatureDesc(hiveFeat);
			if(featDesc.isEmpty()){
				logger.debug("Have to add a hbase feature name");
				String newName = getNewHBaseName();
				ans.put(hiveFeat, newName);
				addFeature(hiveFeat, newName,hiveFeatures.get(hiveFeat), "unknown",null);
			}else if(!featDesc.get(2).equalsIgnoreCase(hiveFeatures.get(hiveFeat))){
				logger.error("The feature "+featDesc+" already exists and does not have the same type");
				logger.error("Existence feature type: "+featDesc.get(2));
				logger.error("Feature to insert type: "+hiveFeatures.get(hiveFeat));
				return null;
			}else{
				ans.put(featDesc.get(0),featDesc.get(1));
			}
		}
		return ans;
	}

	/**
	 * Get a new hbase id not used yet
	 * 
	 * A hbase id is a string: *[0-9a-zA-Z].
	 * The algorithm just increment of one for each new name required
	 * @return
	 */
	private String getNewHBaseName() {
		logger.debug("get a new name...");
		List<String> list = getAllHbaseId();

		long max = 0,
				cur = 0;
		int	i;
		String newName = null,
				maxName = null;
		//First find the right length then increment the last element by one
		Iterator<String> it = list.iterator();
		while(it.hasNext()){
			String name = it.next();
			cur = 0;
			for(i=0;i<name.length();++i){
				cur +=  (long) (((int)name.charAt(i))*Math.pow(128,name.length() -1 - i));
			}
			if(cur > max){
				max = cur;
				maxName = name;
			}
		}
		if(maxName == null){
			newName = "0";
		}else{
			i = maxName.length()-1;
			boolean end = false;
			while(i >= 0 && !end){
				if(maxName.charAt(i) < 122){
					end = true;
				}else{
					--i;
				}
			}
			if(end){
				char c = maxName.charAt(i);
				++c;
				//Managing the bordure
				if(c == 58){
					//After 9, have to go to A
					c = 65;
				}else if(c == 91){
					//After Z, have to go to a
					c = 97;
				}
				newName = maxName.substring(0,i)+c;
				for(++i; i< maxName.length();++i){
					newName += "0";
				}
			}else{
				newName = "0";
				for(i=0;i<maxName.length();++i){
					newName += "0";
				}
			}
		}

		logger.debug("Max: "+maxName+", answer: "+newName);
		return newName;
	}

	/**
	 * Remove features from the dictionary table.
	 * 
	 * The features are not really remove but renamed as
	 * "trash" feature, we avoid to reuse a hbase id like
	 * that.
	 * 
	 * @param features
	 * @return
	 */
	public boolean removeFeaturesFromDicTable(Map<String,String> features){
		boolean ok = true;
		String characters = "1234567890";
		Random rnd = new Random();

		if(!catExists("trash")){
			//Add Category
			ok = addCategory("trash", "");
		}
		List<String> featureNames = getAllFeatures();
		Iterator<String> featureNameIt = features.keySet().iterator();
		int max_iter = 100;
		while(featureNameIt.hasNext() && ok){
			String featureName = featureNameIt.next();
			String type = features.get(featureName);
			String newName = null;
			
			int iter = 0;
			do{
				//Find a name
				char[] randNb = new char[10];
				for (int i = 0; i < randNb.length; i++){
					randNb[i] =  characters.charAt(rnd.nextInt(characters.length()));
				}
				newName = "trash-"+new String(randNb);
				++iter;
			}while(featureNames.contains(newName) && iter<max_iter);
			
			if(iter == max_iter){
				logger.error("Cannot find a suitable trash name");
				ok = false;
			}else{
				//Rename HBaseFeature
				ok = changeHBaseFeature(featureName, type, newName, "trash");
			}
		}

		return ok;
	}

	/**
	 * Rename a hbase feature.
	 * 
	 * Rename/reset a hbase feature property.
	 * 
	 * @param oldName
	 * @param type
	 * @param newName
	 * @param newCategory
	 * @return
	 */
	public boolean changeHBaseFeature(String oldName,String type,String newName,String newCategory){
		boolean ok = false;
		
		if(featExists(oldName)){
			Map<String,String> feature = new LinkedHashMap<String,String>();
			feature.put(oldName, type);
			Map<String,String> hbaseId = getMapFeatures(feature); 
			if(hbaseId != null){
				ok = true;
				String statement = "UPDATE "+HBaseDatabase.dictionary_table +
						" SET "+HBaseDatabase.dictionary_f_name+" = '"+newName+"', "+
						HBaseDatabase.dictionary_f_category+" = '"+newCategory+"'"+
						" WHERE "+HBaseDatabase.dictionary_f_name+" = '"+oldName+"' AND "+
						HBaseDatabase.dictionary_f_type+" = '"+type+"'";
				logger.info(statement);
				try{
					conn.execute(statement);
				}catch(SQLException e){
					ok = logErrorQuery(e);
				}
			}
		}
		if(ok){
			logger.info("Rename the feature '"+oldName+"' to '"+newName+"' in the category '"+newCategory+"'");
		}

		return ok;
	}

	/**
	 * @return the conn
	 */
	public JdbcConnection getConn() {
		return conn;
	}


}
