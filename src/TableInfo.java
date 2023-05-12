import java.io.Serializable;
import java.util.ArrayList;

public class TableInfo implements Serializable {
	String tableName;
	String clusteringKeyName;
	ArrayList<String> clusteringKeyData;
	ArrayList<String> tablePages;
	int pageCount;

	public TableInfo() {
		this.clusteringKeyData = new ArrayList<String>();
		this.tablePages = new ArrayList<String>();
		this.pageCount = -1;
		this.clusteringKeyName = null;
		this.tableName = null;
	}

}