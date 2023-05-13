import java.io.FileInputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

class Cube {
    Hashtable<String, Object> mins, maxs; // <column, minValue>

    public Cube(Hashtable<String, Object> mins, Hashtable<String, Object> maxs) {
        this.mins = mins;
        this.maxs = maxs;
    }
}

class OctTree {
    int maxEntries;
    int entriesCount;
    OctTree[] children;
    Cube bounds;
    // add references to pages

    public OctTree(Cube bounds) throws IOException {
        this.children = new OctTree[8];
        this.bounds = bounds;
        this.entriesCount = 0;

    }

}

public class OctTreeIndex {
    String indexName;
    String tableName;
    String[] columnsNames;
    int maxEntriesPerOctant;
    OctTree octTree;

    public OctTreeIndex() throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("resources/DBApp.config");
        prop.load(fis);

        this.maxEntriesPerOctant = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));

    }

}