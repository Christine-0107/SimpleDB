package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page. 实现锁机制，bufferPool要判断事务是否有读写某一页的锁
 *
 * @Threadsafe, all fields are final
 */
//BufferPool将内存最近读过的物理页缓存下来，所有读写操作通过BufferPool进行
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE; //页的大小

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages; //页的数量
    private ConcurrentHashMap<PageId,Page> buffer;
    private LockManager lockManager;

    /**
     * Lab4
     * 编写辅助类Lock，表示事务拥有的锁的类型
     * */
    private class Lock{
        public static final int SHARE=0;
        public static final int EXCLUSIVE=1;
        private TransactionId tid; //事务id
        private int type; //事务的锁的类型

        public Lock(TransactionId tid,int type){
            this.tid=tid;
            this.type=type;
        }
        public TransactionId getTid(){
            return tid;
        }
        public int getType(){
            return type;
        }
        public void setType(int type){
            this.type=type;
        }

    }

    /**
     * Lab4
     * 编写辅助类LockManager，管理锁
     * */
    private class LockManager{
        //pageId和锁的映射，记录page上现有的锁
        ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,Lock>> lockMap;
        public LockManager(){
            lockMap=new ConcurrentHashMap<>();
        }
        //方法1：获取锁
        //pid该页id，tid需要锁的事务，lockType需要的锁的类型
        public synchronized boolean acquireLock(PageId pid,TransactionId tid,int lockType)
                throws TransactionAbortedException, InterruptedException {
            /** 情况1：如果页面上没有锁，直接获取锁*/
            if(lockMap.get(pid)==null){
                Lock newLock=new Lock(tid,lockType); //新添加一个锁
                ConcurrentHashMap<TransactionId,Lock> pageLocks=new ConcurrentHashMap<>();
                pageLocks.put(tid,newLock);
                lockMap.put(pid,pageLocks); //将它放到LockMap里
                return true; //返回获取锁成功
            }

            //其余情况均为该页上已经存在锁
            ConcurrentHashMap<TransactionId,Lock> pageLocks= lockMap.get(pid);

            /** 情况2：页面上有锁，并且有事务tid的锁*/
            if(pageLocks.get(tid)!=null){
                Lock pageLock=pageLocks.get(tid);
                /** 情况2.1：有事务tid的读锁*/
                if(pageLock.getType()==Lock.SHARE){
                    /** 情况2.1.1：有事务tid的读锁，且需要的也是读锁*/
                    if(lockType==Lock.SHARE){
                        return true; //直接获取，返回成功
                    }
                    /** 情况2.1.2：有事务tid的读锁，但需要的是写锁*/
                    if(lockType==Lock.EXCLUSIVE){
                        /** 情况2.1.2.1：页面上只有tid的读锁*/
                        if(pageLocks.size()==1){
                            pageLock.setType(Lock.EXCLUSIVE); //将它升级为写锁
                            pageLocks.put(tid,pageLock);
                            return true; //添加到锁的列表中，返回成功
                        }
                        /** 情况2.1.2.2：页面上还有其他事务的读锁*/
                        if(pageLocks.size()>1){
                            //不能进行锁的升级，抛出异常
                            throw new TransactionAbortedException();
                        }
                    }
                }
                /** 情况2.2：有事务tid的写锁*/
                if(pageLock.getType()==Lock.EXCLUSIVE){
                    return true; //直接获取
                }
            }

            /** 情况3：页面上有锁，但没有事务tid的锁*/
            if(pageLocks.get(tid)==null){
                /** 情况3.1：页面上锁的个数大于1*/
                if(pageLocks.size()>1){
                    /** 情况3.1.1：tid请求的是读锁*/
                    if(lockType==Lock.SHARE){
                        //添加一个tid的读锁即可
                        Lock newReadLock=new Lock(tid,Lock.SHARE);
                        pageLocks.put(tid,newReadLock);
                        lockMap.put(pid,pageLocks);
                        return true;
                    }
                    /** 情况3.1.2：tid请求的是写锁*/
                    if(lockType==Lock.EXCLUSIVE){
                        //需要等待，不能获取
                        wait(20);
                        return false;
                    }
                }
                /** 情况3.2：页面上锁的个数等于1*/
                if(pageLocks.size()==1){
                    //获取这个锁
                    Lock thisLock=null;
                    for(Lock l:pageLocks.values()){
                        thisLock=l;
                    }
                    /** 情况3.2.1：这个锁为读锁*/
                    if(thisLock.getType()==Lock.SHARE){
                        /** 情况3.2.1.1：这个锁为读锁，需要的也是读锁*/
                        if(lockType==Lock.SHARE){
                            Lock newReadLock=new Lock(tid,Lock.SHARE);
                            pageLocks.put(tid,newReadLock);
                            lockMap.put(pid,pageLocks);
                            return true;
                        }
                        /** 情况3.2.1.2：这个锁为读锁，需要的是写锁*/
                        if(lockType==Lock.EXCLUSIVE){
                            //不能获取，等待一段时间
                            wait(10);
                            return false;
                        }
                    }
                    /** 情况3.2.2：这个锁为写锁*/
                    if(thisLock.getType()==Lock.EXCLUSIVE){
                        //不能获取，等待一段时间
                        wait(10);
                        return false;
                    }
                }
            }
            return false;
        }

        //方法2：判断事务tid是否持有pid上的锁
        public synchronized boolean holdsLock(TransactionId tid,PageId pid){
            ConcurrentHashMap<TransactionId,Lock> pageLocks;
            pageLocks=lockMap.get(pid);
            if(pageLocks==null){
                return false;
            }
            Lock lock=pageLocks.get(tid);
            if(lock==null){
                return false;
            }
            return true;
        }

        //方法3：释放事务tid在某一页上的所有锁
        public synchronized boolean releasePage(TransactionId tid,PageId pid){
            if (holdsLock(tid,pid)){ //tid持有pid上的锁
                ConcurrentHashMap<TransactionId,Lock> pageLocks = lockMap.get(pid);
                pageLocks.remove(tid);
                if (pageLocks.size() == 0){
                    lockMap.remove(pid);
                }
                this.notifyAll(); //释放后唤醒所有等待的线程
                return true;
            }
            return false;

        }
    }



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages=numPages;
        this.buffer=new ConcurrentHashMap<>(numPages);
        this.lockManager=new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions. 用相关许可检索特定的页
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should 该页在buffer中就返回
     * be added to the buffer pool and returned.  If there is insufficient 不在就添加然后返回
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place. 空间不够就驱逐再添加（本次只需抛出异常）
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */

    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        /**Lab4*/
        int type = 0;
        if (perm == Permissions.READ_ONLY) {
            type = Lock.SHARE;
        }
        else {
            type = Lock.EXCLUSIVE;
        }
        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout=new Random().nextInt(2000)+1000;
        //对每个事务设置一个获取锁的超时时间，如果在超时时间内获取不到锁，认为可能发生了死锁
        while(!lockAcquired)
        {
            try
            {
                if(lockManager.acquireLock(pid,tid,type))
                    break;
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if(now-start>timeout)
            {
                throw new TransactionAbortedException();
            }
        }

        if(buffer.containsKey(pid)){ //若该页在buffer中就返回
            return buffer.get(pid);
        }
        else if(buffer.size()<numPages){ //若该页不在buffer中，且buffer中空间充足
            Page newPage=Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            //通过目录定位文件，读取相应页
            buffer.put(pid,newPage); //在buffer中添加该页,然后返回
            return newPage;
        }
        else{ //若该页不在buffer中，且buffer中空间不足
            evictPage();
            Page newPage=Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            buffer.put(pid,newPage);
            return newPage;
            //throw new DbException("Buffer Pool is full"); //在本实验中只用抛出异常，不用写驱逐函数
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     *            一定是commit，调用transactionComplete(tid,true)
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        /**Lab4*/
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     *               commit为真是commit，
     *               为假是abort（从BufferPool中清除掉该事务造成的脏页，并将原始版本重新读到BufferPool中）
     */

    /**编写辅助函数1：帮助写与事务tid相关的脏页到磁盘*/
    private synchronized void flushTidPage(TransactionId tid) throws IOException {
        //找到属于事务tid的所有脏页
        for(PageId pid: buffer.keySet()){
            Page page=buffer.get(pid);
            if(page.isDirty()==tid){
                flushPage(pid);
            }
        }
    }

    /**编写辅助函数2：帮助回滚。从磁盘上读取原始版本的页*/
    private synchronized void restorePage(TransactionId tid){
        //找到属于事务tid的所有脏页
        for(PageId pid: buffer.keySet()){
            Page page=buffer.get(pid);
            if(page.isDirty()==tid){
                int tableId= pid.getTableId(); //找到该页对应的表的id
                DbFile file=Database.getCatalog().getDatabaseFile(tableId); //通过表id定位文件
                Page pageInDisk= file.readPage(pid); //读取该文件中的该页
                buffer.put(pid,pageInDisk);//将该页放回buffer
            }
        }
    }

    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        /**Lab4*/
        if(commit){ //commit==true, 提交成功，将与tid相关脏页写回磁盘
            flushTidPage(tid);
        }
        else{ //commit==false，abort，回滚
            restorePage(tid);
        }
        //释放所有与事务tid相关的锁
        for(PageId pid: buffer.keySet()){
            if(holdsLock(tid,pid)){
                lockManager.releasePage(tid,pid);
            }
        }


    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     * 标记脏页
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageList=new ArrayList<>();
        pageList= file.insertTuple(tid,t);
        for(Page p:pageList){
            p.markDirty(true,tid); //标记脏页
            if(buffer.size()>numPages){
                evictPage();
            }
            buffer.put(p.getId(),p);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pageList=new ArrayList<>();
        pageList= file.deleteTuple(tid,t);
        for(Page p:pageList){
            p.markDirty(true,tid);
            if(buffer.size()>numPages){
                evictPage();
            }
            buffer.put(p.getId(),p);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page page:buffer.values()){
            flushPage(page.getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        buffer.remove(pid);

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(buffer.containsKey(pid)){
            Page page=buffer.get(pid);
            if(page.isDirty()!=null){
                Database.getCatalog().getDatabaseFile(page.getId().getTableId())
                        .writePage(page);
                page.markDirty(false,null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        /**Lab4*/
        ArrayList<PageId> pids=new ArrayList<>(buffer.keySet());
        for(PageId pid:pids){
            if(buffer.get(pid).isDirty()!=null){ //获取到的是脏页，不能evict，找下一个
                continue;
            }
            else{ //获取到的不是脏页，可以evict
                try {
                    flushPage(pid);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                discardPage(pid);
                return;
            }
        }
        throw new DbException("ALL dirty");

    }

}
