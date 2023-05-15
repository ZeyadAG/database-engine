import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

public class ResultSet implements Iterator {
    ArrayList<Tuple> tuples;
    private int pointer;

    public ResultSet() {
        this.tuples = new ArrayList<Tuple>();
        this.pointer = -1;
    }

    public boolean hasNext() {
        if (pointer < tuples.size() - 1)
            return true;
        return false;
    }

    public Tuple next() {
        if (pointer == tuples.size() - 1)
            return null;
        return tuples.get(++pointer);
    }

    public void remove() {
        tuples.remove(pointer);
        pointer--;
    }

    public void resetPointer() {
        pointer = -1;
    }

    public void removeDuplicates() {
        for (int i = 0; i < tuples.size(); i++) {
            Tuple tup = tuples.get(i);

            ArrayList<String> keys = new ArrayList<String>();
            ArrayList<Object> values = new ArrayList<Object>();
            keys.addAll(tup.data.keySet());
            values.addAll(tup.data.values());

            for (int j = 0; j < tuples.size(); j++) {
                Tuple compareTup = tuples.get(j);
                ArrayList<String> compKeys = new ArrayList<String>();
                ArrayList<Object> compValues = new ArrayList<Object>();
                compKeys.addAll(compareTup.data.keySet());
                compValues.addAll(compareTup.data.values());

                if (keys.equals(compKeys) && values.equals(compValues) && i != j) {
                    tuples.remove(i);

                }
            }
        }

    }

    // public void removeDuplicates() {
    // int size = tuples.size();
    // for (int i = 0; i < size - 1; i++) {
    // Tuple currentTuple = tuples.get(i);
    // for (int j = i + 1; j < size; j++) {
    // if (currentTuple.equals(tuples.get(j))) {
    // tuples.remove(j);
    // size--;
    // j--;
    // }
    // }
    // }
    // pointer = -1;
    // }

    public static void main(String[] args) {
        ResultSet resultSet = new ResultSet();
        ResultSet res = new ResultSet();

        Hashtable<String, Object> t1 = new Hashtable<String, Object>();
        t1.put("id", 1);
        t1.put("gpa", 2.0);
        t1.put("name", "Mostafa");
        Tuple t11 = new Tuple(t1);

        Hashtable<String, Object> t2 = new Hashtable<String, Object>();
        t2.put("id", 1);
        t2.put("gpa", 2.0);
        t2.put("name", "Mostafa");
        Tuple t22 = new Tuple(t2);

        Hashtable<String, Object> t3 = new Hashtable<String, Object>();
        t3.put("id", 3);
        t3.put("gpa", 3.0);
        t3.put("name", "Seif");
        Tuple t33 = new Tuple(t3);

        resultSet.tuples.add(new Tuple(t1));
        resultSet.tuples.add(new Tuple(t3));
        resultSet.tuples.add(new Tuple(t2));

        while (resultSet.hasNext()) {
            System.out.print("< ");

            Tuple temp = (Tuple) resultSet.next();
            Enumeration<String> e = temp.data.keys();
            while (e.hasMoreElements()) {
                String key = e.nextElement();
                System.out.print(key + ": " + temp.data.get(key));
                if (e.hasMoreElements())
                    System.out.print(" , ");
            }
            System.out.print(" > \n");

        }

        resultSet.removeDuplicates();
        // resultSet.tuples.clear();
        // resultSet.tuples.addAll(nodDup);

        resultSet.resetPointer();
        System.out.println("-----");
        while (resultSet.hasNext()) {
            System.out.print("< ");

            Tuple temp = (Tuple) resultSet.next();
            Enumeration<String> e = temp.data.keys();
            while (e.hasMoreElements()) {
                String key = e.nextElement();
                System.out.print(key + ": " + temp.data.get(key));
                if (e.hasMoreElements())
                    System.out.print(" , ");
            }
            System.out.print(" > \n");

        }
    }
}
