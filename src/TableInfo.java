import java.io.Serializable;
import java.util.ArrayList;

public class TableInfo implements Serializable {
	String tableName;
	String clusteringKeyName;
	ArrayList<Integer> clusteringKeyData;
	ArrayList<String> tablePages;
	int pageCount;

	public TableInfo() {
		this.clusteringKeyData = new ArrayList<Integer>();
		this.tablePages = new ArrayList<String>();
		this.pageCount = -1;
		this.clusteringKeyName = null;
		this.tableName = null;
	}

	public static void main(String[] args) {

	}
}