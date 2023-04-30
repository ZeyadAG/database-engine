import java.util.Hashtable;

public class Table {
	String tableName;
	String clusterKey;  
	Hashtable<String,String> columns;   
	Hashtable<String,String> minValues;
	Hashtable<String,String> maxValues;

	
	public Table(String strTableName, String strClusteringKeyColumn, 
			Hashtable<String,String> htblColNameType, 
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax) 
	{	
		this.tableName = strTableName;
		this.clusterKey = strClusteringKeyColumn;
		this.columns = htblColNameType;
		this.minValues = htblColNameMin;
		this.maxValues = htblColNameMax;
		
	}

}
