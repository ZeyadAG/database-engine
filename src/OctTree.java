import java.util.ArrayList;
import java.util.List;

class GameObject {
    private float x;
    private float y;
    private float z;
    private float width;
    private float height;
    private float depth;

    public GameObject(float x, float y, float z, float width, float height, float depth) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.width = width;
        this.height = height;
        this.depth = depth;
    }

    public float getX() {
        return this.x;
    }

    public void setX(float x) {
        this.x = x;
    }

    public float getY() {
        return this.y;
    }

    public void setY(float y) {
        this.y = y;
    }

    public float getZ() {
        return this.z;
    }

    public void setZ(float z) {
        this.z = z;
    }

    public float getWidth() {
        return this.width;
    }

    public void setWidth(float width) {
        this.width = width;
    }

    public float getHeight() {
        return this.height;
    }

    public void setHeight(float height) {
        this.height = height;
    }

    public float getDepth() {
        return this.depth;
    }

    public void setDepth(float depth) {
        this.depth = depth;
    }

    public AABB getAABB() {
        return new AABB(this.x, this.y, this.z, this.width, this.height, this.depth);
    }
    /*
     * This implementation stores the position and dimensions of the GameObject, and
     * provides methods to retrieve and set these values. The getAABB method returns
     * an AABB object representing the bounding box of the GameObject.
     */
}

class AABB {
    private float x;
    private float y;
    private float z;
    private float width;
    private float height;
    private float depth;

    public AABB(float x, float y, float z, float width, float height, float depth) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.width = width;
        this.height = height;
        this.depth = depth;
    }

    public float getX() {
        return this.x;
    }

    public float getY() {
        return this.y;
    }

    public float getZ() {
        return this.z;
    }

    public float getWidth() {
        return this.width;
    }

    public float getHeight() {
        return this.height;
    }

    public float getDepth() {
        return this.depth;
    }

    public boolean intersects(AABB other) {
        if (this.x + this.width < other.getX() || other.getX() + other.getWidth() < this.x) {
            return false;
        }

        if (this.y + this.height < other.getY() || other.getY() + other.getHeight() < this.y) {
            return false;
        }

        if (this.z + this.depth < other.getZ() || other.getZ() + other.getDepth() < this.z) {
            return false;
        }

        return true;
    }
    /*
     * This implementation stores the position and dimensions of the AABB, and
     * provides methods to retrieve these values. The intersects method checks if
     * this AABB intersects with another AABB by checking if their projections
     * overlap on each axis.
     */
}

public class OctTree {
    private final int MAX_OBJECTS = 10;
    private final int MAX_LEVELS = 5;

    private int level;
    private List<GameObject> objects;
    private AABB bounds;
    private OctTree[] nodes;

    public OctTree(int level, AABB bounds) {
        this.level = level;
        this.objects = new ArrayList<>();
        this.bounds = bounds;
        this.nodes = new OctTree[8];
    }

    public void clear() {
        this.objects.clear();

        for (int i = 0; i < this.nodes.length; i++) {
            if (this.nodes[i] != null) {
                this.nodes[i].clear();
                this.nodes[i] = null;
            }
        }
    }

    private void split() {
        float subWidth = this.bounds.getWidth() / 2;
        float subHeight = this.bounds.getHeight() / 2;
        float subDepth = this.bounds.getDepth() / 2;
        float x = this.bounds.getX();
        float y = this.bounds.getY();
        float z = this.bounds.getZ();

        this.nodes[0] = new OctTree(this.level + 1, new AABB(x + subWidth, y, z, subWidth, subHeight, subDepth));
        this.nodes[1] = new OctTree(this.level + 1, new AABB(x, y, z, subWidth, subHeight, subDepth));
        this.nodes[2] = new OctTree(this.level + 1, new AABB(x, y + subHeight, z, subWidth, subHeight, subDepth));
        this.nodes[3] = new OctTree(this.level + 1,
                new AABB(x + subWidth, y + subHeight, z, subWidth, subHeight, subDepth));
        this.nodes[4] = new OctTree(this.level + 1,
                new AABB(x + subWidth, y, z + subDepth, subWidth, subHeight, subDepth));
        this.nodes[5] = new OctTree(this.level + 1, new AABB(x, y, z + subDepth, subWidth, subHeight, subDepth));
        this.nodes[6] = new OctTree(this.level + 1,
                new AABB(x, y + subHeight, z + subDepth, subWidth, subHeight, subDepth));
        this.nodes[7] = new OctTree(this.level + 1,
                new AABB(x + subWidth, y + subHeight, z + subDepth, subWidth, subHeight, subDepth));
    }

    private int getIndex(GameObject object) {
        int index = -1;
        float verticalMidpoint = this.bounds.getX() + (this.bounds.getWidth() / 2);
        float horizontalMidpoint = this.bounds.getY() + (this.bounds.getHeight() / 2);
        float depthMidpoint = this.bounds.getZ() + (this.bounds.getDepth() / 2);

        boolean topQuadrant = (object.getY() < horizontalMidpoint
                && object.getY() + object.getHeight() < horizontalMidpoint);
        boolean bottomQuadrant = (object.getY() > horizontalMidpoint);

        if (object.getX() < verticalMidpoint && object.getX() + object.getWidth() < verticalMidpoint) {
            if (topQuadrant) {
                index = 1;
            } else if (bottomQuadrant) {
                index = 2;
            }
        } else if (object.getX() > verticalMidpoint) {
            if (topQuadrant) {
                index = 0;
            } else if (bottomQuadrant) {
                index = 3;
            }
        }

        if (object.getZ() < depthMidpoint && object.getZ() + object.getDepth() < depthMidpoint) {
            if (index == 0) {
                index = 4;
            } else if (index == 1) {
                index = 5;
            } else if (index == 2) {
                index = 6;
            } else if (index == 3) {
                index = 7;
            }
        }

        return index;
    }

    public void insert(GameObject object) {
        if (this.nodes[0] != null) {
            int index = getIndex(object);

            if (index != -1) {
                this.nodes[index].insert(object);

                return;
            }
        }

        this.objects.add(object);

        if (this.objects.size() > MAX_OBJECTS && this.level < MAX_LEVELS) {
            if (this.nodes[0] == null) {
                split();
            }

            int i = 0;

            while (i < this.objects.size()) {
                int index = getIndex(this.objects.get(i));

                if (index != -1) {
                    this.nodes[index].insert(this.objects.remove(i));
                } else {
                    i++;
                }
            }
        }
    }

    public List<GameObject> retrieve(GameObject object) {
        List<GameObject> returnObjects = new ArrayList<>();
        int index = getIndex(object);

        if (index != -1 && this.nodes[0] != null) {
            returnObjects.addAll(this.nodes[index].retrieve(object));
        }

        returnObjects.addAll(this.objects);

        return returnObjects;
    }
    /*
     * This implementation includes the GameObject and AABB classes, and provides
     * methods to insert objects into the OctTree, retrieve objects that could
     * potentially collide with a given object, and clear the OctTree. The split
     * method creates eight child nodes for the OctTree, and the getIndex method
     * determines which child node a given object belongs to. The insert method
     * recursively inserts objects into the appropriate child nodes, and splits the
     * OctTree if necessary. The retrieve method recursively retrieves all objects
     * that could potentially collide with a given object.
     */

    public void delete(GameObject object) {
        int index = getIndex(object);

        if (index != -1 && this.nodes[0] != null) {
            this.nodes[index].delete(object);
        } else {
            this.objects.remove(object);
        }

        // Merge nodes if they contain less than MAX_OBJECTS
        if (this.nodes[0] != null) {
            int totalObjects = 0;

            for (int i = 0; i < this.nodes.length; i++) {
                totalObjects += this.nodes[i].objects.size();
            }

            if (totalObjects <= MAX_OBJECTS) {
                for (int i = 0; i < this.nodes.length; i++) {
                    this.objects.addAll(this.nodes[i].objects);
                    this.nodes[i].clear();
                    this.nodes[i] = null;
                }
            }
        }
        /*
         * This implementation first determines which child node the object belongs to
         * using the getIndex method. If the object is found in a child node, the delete
         * method is called recursively on that child node. Otherwise, the object is
         * removed from the current node's objects list. After deleting the object, the
         * method checks if any child nodes contain less than MAX_OBJECTS, and if so,
         * merges those nodes back into the current node by adding their objects to the
         * current node's objects list and clearing the child nodes.
         */
    }

    public static void displayOctTree(OctTree octTree, int level) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < level; i++) {
            sb.append("  ");
        }

        sb.append("Level ").append(level).append(": ");

        if (octTree.nodes[0] != null) {
            sb.append("Branch");
            System.out.println(sb.toString());

            for (int i = 0; i < octTree.nodes.length; i++) {
                displayOctTree(octTree.nodes[i], level + 1);
            }
        } else {
            sb.append("Leaf (").append(octTree.objects.size()).append(" objects)");
            System.out.println(sb.toString());

            for (GameObject object : octTree.objects) {
                System.out.println("  " + object.toString());
            }
        }
        /*
         * This implementation uses recursion to display the OctTree and its objects in
         * the console. The displayOctTree method takes an OctTree and a level
         * parameter, which is used to indent the output based on the level of the
         * OctTree. If the OctTree has child nodes, the method recursively calls itself
         * on each child node with an increased level. If the OctTree is a leaf node,
         * the method outputs the number of objects in the node and then outputs each
         * object's information. Here's an example usage:
         */
    }

    // main
    public static void main(String[] args) {
        // Create an OctTree with a bounding box of size 100x100x100
        OctTree octTree = new OctTree(0, new AABB(0, 0, 0, 100, 100, 100));

        // Insert some test objects
        GameObject object1 = new GameObject(10, 10, 10, 5, 5, 5);
        GameObject object2 = new GameObject(20, 20, 20, 5, 5, 5);
        GameObject object3 = new GameObject(30, 30, 30, 5, 5, 5);
        GameObject object4 = new GameObject(40, 40, 40, 5, 5, 5);
        GameObject object5 = new GameObject(50, 50, 50, 5, 5, 5);
        GameObject object6 = new GameObject(60, 60, 60, 5, 5, 5);
        GameObject object7 = new GameObject(70, 70, 70, 5, 5, 5);
        GameObject object8 = new GameObject(80, 80, 80, 5, 5, 5);
        GameObject object9 = new GameObject(90, 90, 90, 5, 5, 5);

        octTree.insert(object1);
        octTree.insert(object2);
        octTree.insert(object3);
        octTree.insert(object4);
        octTree.insert(object5);
        octTree.insert(object6);
        octTree.insert(object7);
        octTree.insert(object8);
        octTree.insert(object9);

        // Retrieve objects that could potentially collide with object1
        List<GameObject> potentialCollisions = octTree.retrieve(object1);

        // Print the number of potential collisions
        System.out.println("Number of potential collisions: " + potentialCollisions.size());

        // Delete object1 and retrieve potential collisions again
        octTree.delete(object1);
        potentialCollisions = octTree.retrieve(object1);

        // Print the number of potential collisions again
        System.out.println("Number of potential collisions after deleting object1: " + potentialCollisions.size());

        // Display the OctTr ee
        displayOctTree(octTree, 0);
    }

    /*
     * This main method creates an OctTree with a bounding box of size 100x100x100,
     * inserts some test objects, retrieves objects that could potentially collide
     * with object1, deletes object1, and retrieves potential collisions again. The
     * output should be:
     * 
     * Copy codeNumber of potential collisions: 9
     * Number of potential collisions after deleting object1: 8
     */
}