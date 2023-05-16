import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Date;

class Cube {
    // Hashtable<String, Object> mins, maxs; // <column, minValue>
    Object minC1, minC2, minC3, maxC1, maxC2, maxC3;
    Object midC1, midC2, midC3;

    // public Cube(Hashtable<String, Object> mins, Hashtable<String, Object> maxs) {
    public Cube(Object minC1, Object maxC1, Object minC2, Object maxC2, Object minC3, Object maxC3) {
        this.minC1 = minC1;
        this.minC2 = minC2;
        this.minC3 = minC3;
        this.maxC1 = maxC1;
        this.maxC2 = maxC2;
        this.maxC3 = maxC3;

        this.midC1 = getMid(minC1, maxC1);
        this.midC2 = getMid(minC2, maxC2);
        this.midC3 = getMid(minC3, maxC3);

        // this.mins = mins;
        // this.maxs = maxs;

        // Enumeration<String> minsE = mins.keys();
        // this.minC1 = mins.get(minsE.nextElement());
        // this.minC2 = mins.get(minsE.nextElement());
        // this.minC3 = mins.get(minsE.nextElement());

        // Enumeration<String> maxsE = mins.keys();
        // this.maxC1 = maxs.get(maxsE.nextElement());
        // this.maxC2 = maxs.get(maxsE.nextElement());
        // this.maxC3 = maxs.get(maxsE.nextElement());

    }

    public boolean boundsReference(TupleReference tupleRef) {
        return (compareObjects(minC1, tupleRef.c1) && compareObjects(tupleRef.c1, maxC1) &&
                compareObjects(minC2, tupleRef.c2) && compareObjects(tupleRef.c2, maxC2) &&
                compareObjects(minC3, tupleRef.c3) && compareObjects(tupleRef.c3, maxC3));
    }

    public boolean compareObjects(Object tupleVal, Object mid) {
        if (tupleVal instanceof Integer) {
            if ((Integer) tupleVal <= (Integer) mid)
                return true;
            else
                return false;

        } else if (tupleVal instanceof Double) {
            if ((Double) tupleVal <= (Double) mid)
                return true;
            else
                return false;

        } else if (tupleVal instanceof String) {
            ((String) tupleVal).toLowerCase();
            ((String) mid).toLowerCase();

            if (((String) tupleVal).toLowerCase().compareTo(((String) mid).toLowerCase()) <= 0)
                return true;
            else
                return false;

        } else if (tupleVal instanceof Date) {
            if (((Date) tupleVal).compareTo((Date) mid) <= 0)
                return true;
            else
                return false;
        }
        return false;
    }

    public Object getMidPlusOne(Object mid) {
        if (mid instanceof Integer) {
            int oldMid = (int) mid;
            mid = oldMid + 1;
        } else if (mid instanceof Double) {
            mid = (Double) mid + 1.0;
        } else if (mid instanceof String) {
            String midLowerCase = ((String) mid).toLowerCase();
            char[] chars = midLowerCase.toCharArray();
            int n = chars.length;

            if (chars[n - 1] != 'z') {
                int temp = (int) chars[n - 1];
                char tempChar = (char) ++temp;
                chars[n - 1] = tempChar;
            }

            else if (chars[n - 1] == 'z') {
                int i = n - 2;

                while (chars[i] == 'z' && i > 0)
                    i--;

                int temp = (int) chars[i];
                char tempChar = (char) ++temp;
                chars[i] = tempChar;

                for (int j = i + 1; j < n; j++)
                    chars[j] = 'a';

            }
            mid = new String(chars);
        } else if (mid instanceof Date) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime((Date) mid);
            calendar.add(Calendar.DATE, 1);
            mid = calendar.getTime();
        }
        return mid;
    }

    public Object getMid(Object min, Object max) {
        if (min instanceof Integer) {
            return ((Integer) max + (Integer) min) / 2;
        } else if (min instanceof Double) {
            return ((Double) max + (Double) min) / 2.0;
        } else if (min instanceof String) {
            return findStringMedian((String) min, (String) max,
                    Math.max(((String) max).length(), ((String) min).length()));

        } else if (min instanceof Date) {
            long midMillis = (((Date) min).getTime() + ((Date) max).getTime()) / 2;
            return new Date(midMillis);
        }
        return null;
    }

    public static String stringAdapter(String S, String T) {
        String m = "";

        for (int i = S.length(); i < T.length(); i++) {
            m += T.charAt(i);
        }

        S += m;
        return S;
    }

    public static String findStringMedian(String S, String T, int N) {
        if (S.length() != N) {
            S = stringAdapter(S, T);

        } else if (T.length() != N) {
            T = stringAdapter(T, S);
        }
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        for (int i = 0; i <= N; i++) {

            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }
        String m = "";
        for (int i = 1; i <= a1.length - 1; i++) {
            m += (char) (a1[i] + 97);
        }
        return m;
    }

    public boolean intersects(Cube range) {
        return !(compareObjects(maxC1, range.minC1) ||
                compareObjects(range.maxC1, minC1) ||
                compareObjects(maxC2, range.minC2) ||
                compareObjects(range.maxC2, minC2) ||
                compareObjects(maxC3, range.minC3) ||
                compareObjects(range.maxC3, minC3));
    }
}

class OctTree {
    int maxEntries;
    OctTree[] children;
    Cube bounds;
    ArrayList<TupleReference> entries;
    boolean isDivided;

    public OctTree(Cube bounds, int maxEntries) {
        this.maxEntries = maxEntries; // TODO: get real value
        this.children = new OctTree[8];
        this.bounds = bounds;
        this.entries = new ArrayList<TupleReference>();
        this.isDivided = false;
    }

    // INSERT
    public boolean insertTupleReference(TupleReference tupleRef) throws DBAppException {

        if (!bounds.boundsReference(tupleRef))
            return false;

        if (!isDivided) {
            if (entries.size() < maxEntries) {
                entries.add(tupleRef);
                return true;
            }
            subdivide();
        }

        return (children[0].insertTupleReference(tupleRef) ||
                children[1].insertTupleReference(tupleRef) ||
                children[2].insertTupleReference(tupleRef) ||
                children[3].insertTupleReference(tupleRef) ||
                children[4].insertTupleReference(tupleRef) ||
                children[5].insertTupleReference(tupleRef) ||
                children[6].insertTupleReference(tupleRef) ||
                children[7].insertTupleReference(tupleRef));
    }

    public void subdivide() throws DBAppException {
        // subdivide children
        Cube child1Bounds = new Cube(bounds.minC1, bounds.midC1,
                bounds.getMidPlusOne(bounds.midC2), bounds.maxC2,
                bounds.minC3, bounds.midC3);

        Cube child2Bounds = new Cube(bounds.getMidPlusOne(bounds.midC1), bounds.maxC1,
                bounds.getMidPlusOne(bounds.midC2), bounds.maxC2,
                bounds.minC3, bounds.midC3);

        Cube child3Bounds = new Cube(bounds.minC1, bounds.midC1,
                bounds.minC2, bounds.midC2,
                bounds.minC3, bounds.midC3);

        Cube child4Bounds = new Cube(bounds.getMidPlusOne(bounds.midC1), bounds.maxC1,
                bounds.minC2, bounds.midC2,
                bounds.minC3, bounds.midC3);

        Cube child5Bounds = new Cube(bounds.minC1, bounds.midC1,
                bounds.getMidPlusOne(bounds.midC2), bounds.maxC2,
                bounds.getMidPlusOne(bounds.midC3), bounds.maxC3);

        Cube child6Bounds = new Cube(bounds.getMidPlusOne(bounds.midC1), bounds.maxC1,
                bounds.getMidPlusOne(bounds.midC2), bounds.maxC2,
                bounds.getMidPlusOne(bounds.midC3), bounds.maxC3);

        Cube child7Bounds = new Cube(bounds.minC1, bounds.midC1,
                bounds.minC2, bounds.midC2,
                bounds.getMidPlusOne(bounds.midC3), bounds.maxC3);

        Cube child8Bounds = new Cube(bounds.getMidPlusOne(bounds.midC1), bounds.maxC1,
                bounds.minC2, bounds.midC2,
                bounds.getMidPlusOne(bounds.midC3), bounds.maxC3);

        children[0] = new OctTree(child1Bounds, maxEntries);
        children[1] = new OctTree(child2Bounds, maxEntries);
        children[2] = new OctTree(child3Bounds, maxEntries);
        children[3] = new OctTree(child4Bounds, maxEntries);
        children[4] = new OctTree(child5Bounds, maxEntries);
        children[5] = new OctTree(child6Bounds, maxEntries);
        children[6] = new OctTree(child7Bounds, maxEntries);
        children[7] = new OctTree(child8Bounds, maxEntries);

        isDivided = true;

        // remove refs from parent and put them in children
        for (TupleReference tupleRef : entries) {
            boolean isInserted = children[0].insertTupleReference(tupleRef) ||
                    children[1].insertTupleReference(tupleRef) ||
                    children[2].insertTupleReference(tupleRef) ||
                    children[3].insertTupleReference(tupleRef) ||
                    children[4].insertTupleReference(tupleRef) ||
                    children[5].insertTupleReference(tupleRef) ||
                    children[6].insertTupleReference(tupleRef) ||
                    children[7].insertTupleReference(tupleRef);
            if (!isInserted)
                throw new DBAppException("Maximum entries per octant node cannot be 0");
        }

        // clear entries in parent
        entries.clear();

    }

    // SELECT
    public ArrayList<TupleReference> findTupleReference(Cube range, ArrayList<TupleReference> result) {
        // NOTE THAT: Cube range can be used to query a range OR an exact value (point)
        // to get exact value the min & max will be equal for each dimension of the cube
        // so basically the range will be a point in 1-D instead of a cube in 3-D

        if (result == null)
            result = new ArrayList<TupleReference>();

        if (!range.intersects(bounds))
            return result;

        if (isDivided) {
            children[0].findTupleReference(range, result);
            children[1].findTupleReference(range, result);
            children[2].findTupleReference(range, result);
            children[3].findTupleReference(range, result);
            children[4].findTupleReference(range, result);
            children[5].findTupleReference(range, result);
            children[6].findTupleReference(range, result);
            children[7].findTupleReference(range, result);
        }

        for (TupleReference tupleRef : entries) {
            if (range.boundsReference(tupleRef))
                result.add(tupleRef);
        }

        return result;
    }

    // DELETE
    public void deleteTupleReference(Cube range) {

        if (isDivided) {
            children[0].deleteTupleReference(range);
            children[1].deleteTupleReference(range);
            children[2].deleteTupleReference(range);
            children[3].deleteTupleReference(range);
            children[4].deleteTupleReference(range);
            children[5].deleteTupleReference(range);
            children[6].deleteTupleReference(range);
            children[7].deleteTupleReference(range);
        }

        ArrayList<TupleReference> toBeDeleted = new ArrayList<TupleReference>();

        for (TupleReference tupleRef : entries) {
            if (range.boundsReference(tupleRef))
                toBeDeleted.add(tupleRef);
        }

        entries.removeAll(toBeDeleted);

        int count = countReferences();

        if (count <= maxEntries)
            mergeChildrenWithParent();

    }

    public void mergeChildrenWithParent() {
        ArrayList<TupleReference> refs = findTupleReference(bounds, null);

        deleteReferenceNoMerge(bounds);

        isDivided = false;

        children = new OctTree[8];

        entries.addAll(refs);

    }

    public void deleteReferenceNoMerge(Cube range) {

        if (isDivided) {
            children[0].deleteTupleReference(range);
            children[1].deleteTupleReference(range);
            children[2].deleteTupleReference(range);
            children[3].deleteTupleReference(range);
            children[4].deleteTupleReference(range);
            children[5].deleteTupleReference(range);
            children[6].deleteTupleReference(range);
            children[7].deleteTupleReference(range);
        }

        ArrayList<TupleReference> toBeDeleted = new ArrayList<TupleReference>();

        for (TupleReference tupleRef : entries) {
            if (range.boundsReference(tupleRef))
                toBeDeleted.add(tupleRef);
        }

        entries.removeAll(toBeDeleted);

    }

    public int countReferences() {
        int count = this.entries.size();

        if (this.children[0] != null) {
            for (int i = 0; i < this.children.length; i++) {
                count += this.children[i].countReferences();
            }
        }

        return count;
    }

    // UPDATE
    public void updateTupleReference(Cube point, Object[] newValues) throws DBAppException {
        // NOTE THAT: the Cube param will always be passed in as just point in 1-D
        // so oldRef will only be one tuple to update

        ArrayList<TupleReference> oldRef = findTupleReference(point, null);
        TupleReference newRef = oldRef.get(0);

        deleteTupleReference(point);

        newRef.c1 = newValues[0];
        newRef.c2 = newValues[1];
        newRef.c3 = newValues[2];

        insertTupleReference(newRef);
    }

    // HELPER METHODS
    public static void displayOctTree(OctTree octTree, int level) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < level; i++) {
            sb.append("  ");
        }

        sb.append("Level ").append(level).append(": ");

        if (octTree.children[0] != null) {
            sb.append("Branch");
            System.out.println(sb.toString());

            for (int i = 0; i < octTree.children.length; i++) {
                displayOctTree(octTree.children[i], level + 1);
            }
        } else {
            sb.append("Leaf (").append(octTree.entries.size()).append(" objects)");
            System.out.println(sb.toString());

            for (TupleReference tuplRef : octTree.entries) {
                System.out.println("  " + tuplRef.toString());
            }
        }
    }

    public static void main(String[] args) throws DBAppException {
        OctTree ot = new OctTree(new Cube(0, 100, 0, 50, 0, 200), 2);

        Hashtable<String, Object> t1 = new Hashtable<String, Object>();
        t1.put("Col1", 60);
        t1.put("Col2", 30);
        t1.put("Col3", 70);

        Hashtable<String, Object> t2 = new Hashtable<String, Object>();
        t2.put("Col1", 70);
        t2.put("Col2", 40);
        t2.put("Col3", 80);

        Hashtable<String, Object> t3 = new Hashtable<String, Object>();
        t3.put("Col1", 80);
        t3.put("Col2", 45);
        t3.put("Col3", 120);

        Hashtable<String, Object> t4 = new Hashtable<String, Object>();
        t4.put("Col1", 65);
        t4.put("Col2", 35);
        t4.put("Col3", 80);

        Hashtable<String, Object> t5 = new Hashtable<String, Object>();
        t5.put("Col1", 10);
        t5.put("Col2", 10);
        t5.put("Col3", 10);

        Hashtable<String, Object> t6 = new Hashtable<String, Object>();
        t6.put("Col1", 90);
        t6.put("Col2", 40);
        t6.put("Col3", 190);

        // insert
        ot.insertTupleReference(new TupleReference(t1, "pageRefExample"));
        ot.insertTupleReference(new TupleReference(t2, "pageRefExample"));
        ot.insertTupleReference(new TupleReference(t3, "pageRefExample"));
        ot.insertTupleReference(new TupleReference(t4, "pageRefExample"));
        // ot.insertTupleReference(new TupleReference(t5, "pageRefExample"));
        // ot.insertTupleReference(new TupleReference(t6, "pageRefExample"));

        Cube range1 = new Cube(60, 80, 30, 45, 70, 120);

        Cube range2 = new Cube(60, 60, 30, 30, 70, 70);

        Cube range3 = new Cube(70, 70, 40, 40, 80, 80);

        Cube range4 = new Cube(70, 70, 40, 40, 80, 80);

        displayOctTree(ot, 0);

        // select
        ArrayList<TupleReference> result = ot.findTupleReference(range1, null);
        System.out.println("\nfound tuples:");
        for (TupleReference tupleRef : result) {
            System.out.println(tupleRef);
        }
        System.out.println("count: " + ot.countReferences());

        // delete
        // ot.deleteTupleReference(range3);
        // displayOctTree(ot, 0);

        // update
        Object[] newVals = { 85, 50, 120 };
        ot.updateTupleReference(range4, newVals);
        displayOctTree(ot, 0);

        System.out.println("count: " + ot.countReferences());
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

    public OctTreeIndex(String tableName, String indexName, String[] columnsNames) throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("resources/DBApp.config");
        prop.load(fis);
        this.maxEntriesPerOctant = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));

        ArrayList<String[]> tableData = csvReader("metadata.csv", tableName);
        Hashtable<String, Object> mins = new Hashtable<String, Object>();
        Hashtable<String, Object> maxs = new Hashtable<String, Object>();

        for (int i = 0; i < tableData.size(); i++) {
            for (int j = 0; j < columnsNames.length; j++) {
                if (tableData.get(i)[1].equals(columnsNames[j])) {
                    mins.put(columnsNames[j], tableData.get(i)[6]);
                    maxs.put(columnsNames[j], tableData.get(i)[7]);

                }
            }
        }

        // Cube bounds = new Cube(mins, maxs);

    }

    public ArrayList<String[]> csvReader(String fileName, String strTableName) {
        String csvFile = fileName;
        String line = "";
        String csvSeparator = ",";
        ArrayList<String[]> result = new ArrayList<String[]>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvSeparator);
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

}