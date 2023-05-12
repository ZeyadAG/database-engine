import java.util.Hashtable;

class Octant {
    int maxEntries;
    int entriesCount;
    Hashtable<String, String> mins, maxs; // <column, minValue>
    Octant[] children;
    // add references to pages

    public Octant(Hashtable<String, String> mins, Hashtable<String, String> maxs, int maxEntries) {
        this.mins = mins;
        this.maxs = maxs;
        this.maxEntries = maxEntries;
        this.entriesCount = 0;
        this.children = new Octant[8];
    }
}

class OctTree {
    int maxEntries;
    Octant rootOct;

}

public class OctTreeIndex {
    String indexName;
    String tableName;
    String[] columnsNames;
    OctTree octTree;

}