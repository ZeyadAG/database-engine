import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBApp {

	ArrayList<String> tableNames;

	public DBApp() {
	}

	public void init() throws IOException {
		// tableNames = new ArrayList<String>();
	}

	public void createTable(Table t) throws DBAppException, IOException {
		// if (!tableNames.contains(t.tableName))
		// tableNames.add(t.tableName);
		// else
		// throw new DBAppException("table name already exists!"); // mesh hatenfa3 lama
		// ne run el program tany

		File metadataFile = new File("metadata.csv");
		FileWriter outputFile = new FileWriter(metadataFile, true);

		String line, column, columnType, min, max, isClustering;
		Enumeration<String> e = t.columns.keys();

		while (e.hasMoreElements()) {
			column = e.nextElement();
			columnType = t.columns.get(column);
			isClustering = t.clusterKey == column ? "True" : "False";
			min = t.minValues.get(column);
			max = t.maxValues.get(column);

			line = t.tableName + "," + column + "," + columnType + "," + isClustering + "," + "null" + "," + "null"
					+ "," + min + "," + max + "\n";
			outputFile.append(line);
		}
		outputFile.close();

		TableInfo tableInfo = new TableInfo();
		tableInfo.tableName = t.tableName;
		tableInfo.clusteringKeyName = t.clusterKey;
		String tableInfoName = t.tableName + "Info" + ".class";

		writeObject(tableInfoName, tableInfo);
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {
		int clusteringKeyValue = 0;
		int n = 2; // MAX TUPLES PER PAGE
		// int counter = 0;
		// int clusterKeyIndex = 0;
		String clusterKeyName = "";
		ArrayList<String[]> result = csvReader("metadata.csv", strTableName);
		Enumeration<String> x = htblColNameValue.keys();
		while (x.hasMoreElements()) {
			String current = x.nextElement();
			for (int i = 0; i < result.size(); i++) {
				if (current.equals(result.get(i)[1])) {
					Object temp = htblColNameValue.get(current);
					Boolean cluster = Boolean.parseBoolean(result.get(i)[3]);

					if (temp instanceof Double) {
						temp = (double) temp;
						double min = (Double.parseDouble(result.get(i)[6]));
						double max = (Double.parseDouble(result.get(i)[7]));
						if (cluster) {
							clusteringKeyValue = (int) htblColNameValue.get(current);
							// clusterKeyIndex = counter;
							clusterKeyName = result.get(i)[1];
						}
						if ((double) temp < min || (double) temp > max) {
							throw new Exception();
						}
					}
					if (temp instanceof Integer) {
						temp = (int) temp;
						int min = (Integer.parseInt(result.get(i)[6]));
						int max = (Integer.parseInt(result.get(i)[7]));
						if (cluster) {
							clusteringKeyValue = (int) htblColNameValue.get(current);
							// clusterKeyIndex = counter;
							clusterKeyName = result.get(i)[1];

						}
						if ((int) temp < min || (int) temp > max) {
							throw new Exception();
						}
					}
					if (temp instanceof String) {
						temp = (String) temp;
						String min = result.get(i)[6];
						String max = result.get(i)[7];
						if (cluster) {
							clusteringKeyValue = Integer.parseInt((String) htblColNameValue.get(current));
							// clusterKeyIndex = counter;
							clusterKeyName = result.get(i)[1];
						}
						if (((String) temp).compareTo(min) < 0 || ((String) temp).compareTo(max) > 0) {
							throw new Exception();
						}
					}
					if (temp instanceof Date) {
						temp = (Date) temp;
						Date min = new SimpleDateFormat("YYYY-MM-DD").parse(result.get(i)[6]);
						Date max = new SimpleDateFormat("YYYY-MM-DD").parse(result.get(i)[7]);
						if (cluster) {
							clusteringKeyValue = (int) htblColNameValue.get(current);
							// clusterKeyIndex = counter;
							clusterKeyName = result.get(i)[1];

						}
						if (((Date) temp).compareTo(min) < 0 || ((Date) temp).compareTo(max) > 0) {
							throw new Exception();
						}
					}
				}

			}
			// counter++;
		}

		// deserialize table info
		TableInfo tableInfo = (TableInfo) readObject(strTableName + "Info" + ".class");
		String pageName;

		// add clus key to the array of clustering keys

		tableInfo.clusteringKeyData.add(clusteringKeyValue);
		tableInfo.clusteringKeyData.sort(Comparator.naturalOrder());
		writeObject(strTableName + "Info" + ".class", tableInfo);
		System.out.println(tableInfo.clusteringKeyData);

		int index = tableInfo.clusteringKeyData.indexOf(clusteringKeyValue);

		// get the index of the page to load
		int pagePointer = index / n;
		System.out.println("index: " + index); // get REAL n
		System.out.println("pointer: " + pagePointer); // get REAL n

		Tuple tuple = new Tuple(htblColNameValue); // create the tuple with the input hashtable

		try {

			pageName = tableInfo.tablePages.get(pagePointer);

			Page page = (Page) readObject(pageName + ".class"); // deserilaize the page
			System.out.println("no. of tuples(before): " + page.tuples.size());

			if (page.tuples.size() < n) {
				System.out.println("abl insert");

				insertTuple(pageName, tuple, page, clusterKeyName, tableInfo, strTableName);

				System.out.println("ba3d insert");

			} else {
				shiftTuples(pageName, tuple, pagePointer, strTableName, clusterKeyName);
				System.out.println("shifted successfully");
			}

		} catch (IndexOutOfBoundsException e) {
			System.out.println(e.getMessage());
			System.out.println("abl create new page");

			createNewPage(tuple, tableInfo, strTableName);
			System.out.println("ba3d create new page");

			// tableInfo.pageCount++;
			// tableInfo.tablePages.add(strTableName + "_p" + tableInfo.pageCount);
			// pageName = tableInfo.tablePages.get(pagePointer);
			// Page p = new Page(); // create page to add the tuple then serialize it
			// p.tuples.add(tuple); // add the tuple to the tuples array in the page
			// writeObject(pageName+".class", p);

		}
		printPagesContent(tableInfo);
	}

	public void createNewPage(Tuple tuple, TableInfo tableInfo, String strTableName) throws IOException {
		tableInfo.pageCount++;
		tableInfo.tablePages.add(strTableName + "_p" + tableInfo.pageCount);
		String pageName = strTableName + "_p" + tableInfo.pageCount;
		Page p = new Page(); // create page to add the tuple then serialize it
		p.tuples.add(tuple); // add the tuple to the tuples array in the page
		writeObject(pageName + ".class", p); // serialize
		writeObject(strTableName + "Info" + ".class", tableInfo);
		System.out.println("no. of tuples(after): " + p.tuples.size());
	}

	public void shiftTuples(String pageName, Tuple tuple, int pagePointer, String strTableName, String clusterKeyName)
			throws ClassNotFoundException, IOException {
		int n = 2; // read from properties
		TableInfo tableInfo = (TableInfo) readObject(strTableName + "Info" + ".class");
		Page page = (Page) readObject(pageName + ".class"); // deserialize destination page
		if (page.tuples.size() < n) {
			insertTuple(pageName, tuple, page, clusterKeyName, tableInfo, strTableName);
			System.out.println("returned from shift");
			return;
		}

		Tuple tempTuple = page.tuples.get(n - 1);
		page.tuples.remove(n - 1);
		insertTuple(pageName, tuple, page, clusterKeyName, tableInfo, strTableName);

		try {
			pageName = tableInfo.tablePages.get(++pagePointer);
			System.out.println("lol 1");
			shiftTuples(pageName, tempTuple, pagePointer, strTableName, clusterKeyName);
			System.out.println("lol 2");

		} catch (IndexOutOfBoundsException e) {
			System.out.println(e.getMessage());
			System.out.println("lol 3");

			// el mafrood neda5al tempTuple
			createNewPage(tempTuple, tableInfo, strTableName);
			System.out.println("lol 4");

		}

	}

	public static ArrayList<String[]> csvReader(String fileName, String strTableName) {
		String csvFile = fileName;
		String line = "";
		String csvSeparator = ",";
		ArrayList<String[]> result = new ArrayList<String[]>();
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			br.readLine();
			while ((line = br.readLine()) != null) {
				String[] values = line.split(csvSeparator);
				// for (String value : values) {
				// System.out.print(value + " ");
				// }
				// result.add(values);
				//
				// System.out.println();
				if (values[0].equals(strTableName)) {
					result.add(values);
				}

			}
			return result;

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	public void insertTuple(String pageName, Tuple tuple, Page page, String clusteringKeyName, TableInfo tableInfo,
			String strTableName)
			throws ClassNotFoundException, IOException {

		System.out.println("da5al insert");
		int clusterValue = (int) tuple.data.get(clusteringKeyName);

		// binary search on tuples
		int left = 0;
		int right = page.tuples.size() - 1;

		while (left <= right) {
			int mid = left + (right - left) / 2;
			Tuple temp = page.tuples.get(mid);
			int currentCluster = (int) temp.data.get(clusteringKeyName);

			if (left == right) {
				if (currentCluster < clusterValue)
					page.tuples.insertElementAt(tuple, left + 1);
				else
					page.tuples.insertElementAt(tuple, left);
				break;
			}

			if (currentCluster < clusterValue)
				left = mid + 1;
			else
				right = mid - 1;
		}

		writeObject(strTableName + "Info" + ".class", tableInfo);
		writeObject(pageName + ".class", page);
		System.out.println("no. of tuples(after): " + page.tuples.size());
		System.out.println("5arag insert");

	}

	public Object readObject(String path) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(path);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object o = (Object) in.readObject();
		in.close();
		fileIn.close();
		return o;
	}

	public void writeObject(String path, Object obj) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(obj);
		out.close();
		fileOut.close();
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		int n = 2; // GET ACTUAL N
		String clusterKeyName = "";
		ArrayList<String[]> result = csvReader("metadata.csv", strTableName);
		ArrayList<Integer> clusteringKeyData = getClusteringKeyData(strTableName);
		int clusterIndex = Collections.binarySearch(clusteringKeyData, Integer.parseInt(strClusteringKeyValue));
		int pagePointer = clusterIndex / n;

		// get name from pKaza
		String pageName = strTableName + "_p" + pagePointer; // assume page pointer is equivalent to pageCount
		Page page = (Page) readObject(pageName + ".class"); // deserialize required page

		// method to get clustering key name from metadata
		for (int j = 0; j < result.size(); j++) {
			if (result.get(j)[3] == "true") {
				clusterKeyName = result.get(j)[1];
			}
		}

		// method for matching the right tuple with the strClusteringKeyValue and
		// replacing its data
		for (int i = 0; i < page.tuples.size(); i++) {
			Tuple t = page.tuples.get(i);
			if (t.data.get(clusterKeyName) == strClusteringKeyValue) {
				t.data = htblColNameValue; // check if htblColNameValue contains values for everything
			}
		}

		writeObject(pageName + ".class", page); // serialize back

	}

	public ArrayList<Integer> getClusteringKeyData(String tableName) throws ClassNotFoundException, IOException {
		String x = tableName + "Info" + ".class";
		Object t = (TableInfo) readObject(x);
		return ((TableInfo) t).clusteringKeyData;

	}

	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		TableInfo tableInfo = (TableInfo) readObject(strTableName + "Info" + ".class");

		deleteTuples(strTableName, tableInfo, htblColNameValue);

		System.out.println("\nafter tuples deletion");
		printPagesContent(tableInfo);

		writeObject(strTableName + "Info" + ".class", tableInfo);

		tableInfo = (TableInfo) readObject(strTableName + "Info" + ".class");
		orderTuples(tableInfo);
		writeObject(strTableName + "Info" + ".class", tableInfo);

		System.out.println("\nafter ordering tuples");
		printPagesContent(tableInfo);

		tableInfo = (TableInfo) readObject(strTableName + "Info" + ".class");
		deleteEmptyPages(strTableName, tableInfo);

		writeObject(strTableName + "Info" + ".class", tableInfo);

		// System.out.println(tableInfo.tablePages);
		// tableInfo = (TableInfo) readObject(strTableName + "Info" + ".class");
		// System.out.println(tableInfo.tablePages);
		System.out.println("\nafter pages deletion");
		printPagesContent(tableInfo);

	}

	public void deleteTuples(String strTableName, TableInfo tableInfo, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {
		for (int i = 0; i < tableInfo.tablePages.size(); i++) {
			Page page = (Page) readObject(tableInfo.tablePages.get(i) + ".class");
			// int tuplesToDelete = 0;
			for (int j = 0; j < page.tuples.size(); j++) {
				Tuple tuple = page.tuples.get(j);
				Object clusteringKeyValue = tuple.data.get(tableInfo.clusteringKeyName);
				if (checkTupleValues(tuple, htblColNameValue)) {
					// tuplesToDelete++;
					System.out.print("tuple=> " + tableInfo.clusteringKeyName + ": " + clusteringKeyValue + "\n");
					page.tuples.remove(tuple);
					tableInfo.clusteringKeyData.remove(clusteringKeyValue);
					j--;
				}
			}

			// while (tuplesToDelete > 0) {
			// page.tuples.remove(tuple);
			// tableInfo.clusteringKeyData.remove(clusteringKeyValue);
			// }
			writeObject(tableInfo.tablePages.get(i) + ".class", page);
		}
		writeObject(strTableName + "Info" + ".class", tableInfo);
	}

	public void orderTuples(TableInfo tableInfo) throws ClassNotFoundException, IOException {
		int n = 2; // get real max from config
		int pageNotFullIndex = 0;
		ArrayList<String> tablePages = tableInfo.tablePages;
		Page pageNotFull = null;

		// reach the first page that is not full
		for (int i = 0; i < tablePages.size(); i++) {
			Page page = (Page) readObject(tablePages.get(i) + ".class");
			if (page.tuples.size() < n) {
				pageNotFull = page;
				pageNotFullIndex = i;
				writeObject(tableInfo.tablePages.get(i) + ".class", page);
				break;
			}
			writeObject(tableInfo.tablePages.get(i) + ".class", page);
		}

		// base case

		if (pageNotFull == null)
			return;

		// base case

		if (tablePages.get(tablePages.size() - 1).equals(tablePages.get(pageNotFullIndex)))
			return;

		int missingTuples = n - pageNotFull.tuples.size();
		ArrayList<Tuple> tempTuples = new ArrayList<Tuple>();

		int nextPageIndex = tablePages.indexOf(tablePages.get(pageNotFullIndex + 1));
		for (int i = nextPageIndex; i < tablePages.size(); i++) {
			Page page = (Page) readObject(tablePages.get(i) + ".class");
			while (tempTuples.size() < missingTuples) {
				if (page.tuples.isEmpty())
					break;
				tempTuples.add(page.tuples.remove(0));

			}
			if (tempTuples.size() == missingTuples) {
				writeObject(tableInfo.tablePages.get(i) + ".class", page);
				break;
			}

			writeObject(tableInfo.tablePages.get(i) + ".class", page);
		}

		// base case
		if (tempTuples.isEmpty())
			return;

		// insert the tuples from tempTuples to pageNotFull
		int tempTupleSize = tempTuples.size();
		pageNotFull = (Page) readObject(tableInfo.tablePages.get(pageNotFullIndex) + ".class");

		for (int i = 0; i < tempTupleSize; i++)
			pageNotFull.tuples.add(tempTuples.remove(i));

		writeObject(tableInfo.tablePages.get(pageNotFullIndex) + ".class", pageNotFull);

		orderTuples(tableInfo);
	}

	public void deleteEmptyPages(String strTableName, TableInfo tableInfo) throws ClassNotFoundException, IOException {
		// int pagesToDelete = 0;
		ArrayList<String> tablePages = tableInfo.tablePages;

		for (int i = 0; i < tablePages.size(); i++) {
			Page page = (Page) readObject(tablePages.get(i) + ".class");
			if (page.tuples.isEmpty()) {
				// pagesToDelete++;
				File pageFile = new File(tableInfo.tablePages.get(i) + ".class");
				pageFile.delete();
				tableInfo.tablePages.remove(tableInfo.tablePages.get(i));
				i--;
			} else
				writeObject(tableInfo.tablePages.get(i) + ".class", page);
		}
		// while (pagesToDelete > 0) {
		// File pageFile = new File(tablePages.get(tablePages.size() - 1) + ".class");
		// pageFile.delete();
		// tableInfo.tablePages.remove(tablePages.get(tablePages.size() - 1));
		// pagesToDelete--;
		// }
		writeObject(strTableName + "Info" + ".class", tableInfo);

	}

	public boolean checkTupleValues(Tuple tuple, Hashtable<String, Object> htblColNameValue) {
		boolean flag = true;
		Enumeration<String> e = htblColNameValue.keys();
		while (e.hasMoreElements()) {
			String key = e.nextElement();
			Object value = htblColNameValue.get(key);
			Object tupleValue = tuple.data.get(key);
			if (!value.equals(tupleValue))
				flag = false;
		}
		return flag;
	}

	public Page reachFirstNotFullPage(TableInfo tableInfo, int n) throws ClassNotFoundException, IOException {
		for (int i = 0; i < tableInfo.tablePages.size(); i++) {
			Page page = (Page) readObject(tableInfo.tablePages.get(i) + ".class");
			if (page.tuples.size() < n) {
				writeObject(tableInfo.tablePages.get(i) + ".class", page);
				return page;
			}
			writeObject(tableInfo.tablePages.get(i) + ".class", page);
		}
		return null;
	}

	// }

	public void printPagesContent(TableInfo tableInfo) throws ClassNotFoundException, IOException {

		for (int i = 0; i < tableInfo.tablePages.size(); i++) {
			String pageName = tableInfo.tablePages.get(i);
			System.out.print(tableInfo.tableName + "_p" + i + " => " + "{");
			Page page = (Page) readObject(pageName + ".class");
			for (int j = 0; j < page.tuples.size(); j++) {
				System.out.print("[");
				Tuple curTuple = page.tuples.get(j);
				System.out.print("id: " + curTuple.data.get("id"));
				if (j == page.tuples.size() - 1)
					System.out.print("]");
				else
					System.out.print("], ");

			}
			System.out.print("}\n");
			writeObject(tableInfo.tablePages.get(i) + ".class", page);
		}

	}

	// main method
	public static void main(String[] args) throws Exception {
		DBApp engine = new DBApp();
		engine.init();

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");

		Hashtable<String, String> mins = new Hashtable<String, String>();
		mins.put("id", "0");
		mins.put("name", "A");
		mins.put("gpa", "0");

		Hashtable<String, String> maxs = new Hashtable<String, String>();
		maxs.put("id", "9999");
		maxs.put("name", "ZZZZZZZZZZZZZZZZ");
		maxs.put("gpa", "4");

		// data
		Hashtable<String, Object> t1 = new Hashtable<String, Object>();
		t1.put("id", 1);
		t1.put("gpa", 2.0);
		t1.put("name", "Stafa");

		Hashtable<String, Object> t2 = new Hashtable<String, Object>();
		t2.put("id", 2);
		t2.put("gpa", 2.0);
		t2.put("name", "Sdaf");

		Hashtable<String, Object> t3 = new Hashtable<String, Object>();
		t3.put("id", 3);
		t3.put("gpa", 3.0);
		t3.put("name", "Sertaf");

		Hashtable<String, Object> t4 = new Hashtable<String, Object>();
		t4.put("id", 4);
		t4.put("gpa", 3.0);
		t4.put("name", "Seaf");

		Hashtable<String, Object> t5 = new Hashtable<String, Object>();
		t5.put("id", 5);
		t5.put("gpa", 3.0);
		t5.put("name", "Sesdfaf");

		Hashtable<String, Object> t6 = new Hashtable<String, Object>();
		t6.put("id", 6);
		t6.put("gpa", 3.0);
		t6.put("name", "Sesdfaf");

		//
		Hashtable<String, Object> d1 = new Hashtable<String, Object>();
		d1.put("name", "Sesdfaf");

		Hashtable<String, Object> d2 = new Hashtable<String, Object>();
		d2.put("gpa", 3.0);
		d2.put("name", "Seaf");

		Table t = new Table("Student", "id", htblColNameType, mins, maxs);

		engine.createTable(t);
		engine.insertIntoTable("Student", t1);
		engine.insertIntoTable("Student", t2);
		engine.insertIntoTable("Student", t3);
		engine.insertIntoTable("Student", t4);
		engine.insertIntoTable("Student", t5);
		engine.insertIntoTable("Student", t6);

		// engine.deleteFromTable("Student", d2);

		System.out.println("\nin main");
		TableInfo tableInfo = (TableInfo) engine.readObject("Student" + "Info" +
				".class");
		engine.printPagesContent(tableInfo);

		// t1,

	}

}