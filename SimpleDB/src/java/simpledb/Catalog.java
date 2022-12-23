package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
//Catalog中记录数据库中所有表和它们的模式
public class Catalog {

    /*创建一个Table类，存数据库中表的信息，
     *主要包括DBFile(每张表都能用DBFile代表，有一个id用来将表的元数据存到目录，该id即为tableId),
     * name(表名称)
     * primaryKey(主键)*/
    private class Table{
        private final DbFile dbFile;
        private final String tableName;
        private final String primaryKey;

        private Table(DbFile dbFile, String tableName, String primaryKey) {
            this.dbFile = dbFile;
            this.tableName = tableName;
            this.primaryKey = primaryKey;
        }

        public DbFile getDbFile(){
            return dbFile;
        }
        public String getTableName(){
            return tableName;
        }
        public String getPrimaryKey(){
            return primaryKey;
        }

        @Override
        public String toString() {
            return "Table{" +
                    "dbFileID=" + dbFile.getId() +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey='" + primaryKey + '\'' +
                    '}';
        }
    }

    private final ConcurrentHashMap<Integer,Table> hashTable; //为目录建立由标号到表的索引
    private final ConcurrentHashMap<String,Integer> nameToID; //由名称找ID的Map，方便后续函数撰写

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        hashTable=new ConcurrentHashMap<Integer,Table>();
        nameToID=new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table newTable=new Table(file,name,pkeyField);
        int id=file.getId();
        hashTable.put(id,newTable);  //在map里添加一个键值对
        nameToID.put(name,id);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(name!=null){
            Integer id1=nameToID.get(name);
            if(id1!=null){
                return id1;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        DbFile file=getDatabaseFile(tableid); //通过tableid定位到dbFile
        if(file!=null){
            return file.getTupleDesc(); //调用DbFile类的函数找tupleDesc
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table findTable=hashTable.get(tableid); //通过id定位table
        if(findTable!=null){
            DbFile file=findTable.getDbFile();
            return file;
        }
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        Table findTable=hashTable.get(tableid); //通过id定位table
        if(findTable!=null){
            String key=findTable.getPrimaryKey();
            return key;
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return nameToID.values().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table findTable=hashTable.get(id); //通过id定位table
        if(findTable!=null){
            String name=findTable.getTableName();
            return name;
        }
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        hashTable.clear();
        nameToID.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

