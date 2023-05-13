import java.util.ArrayList;
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
}
