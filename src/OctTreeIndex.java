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

    public boolean insertReference(TupleReference tupleRef) throws DBAppException {

        if (!bounds.boundsReference(tupleRef))
            return false;

        if (!isDivided) {
            if (entries.size() < maxEntries) {
                entries.add(tupleRef);
                return true;
            }
            subdivide();
        }

        return (children[0].insertReference(tupleRef) ||
                children[1].insertReference(tupleRef) ||
                children[2].insertReference(tupleRef) ||
                children[3].insertReference(tupleRef) ||
                children[4].insertReference(tupleRef) ||
                children[5].insertReference(tupleRef) ||
                children[6].insertReference(tupleRef) ||
                children[7].insertReference(tupleRef));
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
            boolean isInserted = children[0].insertReference(tupleRef) ||
                    children[1].insertReference(tupleRef) ||
                    children[2].insertReference(tupleRef) ||
                    children[3].insertReference(tupleRef) ||
                    children[4].insertReference(tupleRef) ||
                    children[5].insertReference(tupleRef) ||
                    children[6].insertReference(tupleRef) ||
                    children[7].insertReference(tupleRef);
            if (!isInserted)
                throw new DBAppException("Maximum entries per octant node cannot be 0");
        }

        // clear entries in parent
        entries.clear();
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