package ffp;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public class FFPHashMap <E, V>  { 
 
    /****************************************************************************
     *                           statistics                                     *
     ****************************************************************************/
    private long total_nodes_valid;
    private long total_nodes_invalid;
    private long total_buckets;
    private long total_empties; 
    private long total_min_hash_trie_depth;
    private long total_max_hash_trie_depth;
    private long total_max_nodes;
    private long total_min_nodes;

    private final int MAX_NODES_PER_BUCKET = 6;
    private int SHIFT_SIZE;
    private int BASE_HASH_BUCKETS;
        
    private static final Unsafe unsafe;
    private static final int base; 
    private static final int scale; 
    private static final long next_addr;

    private Object [] HN;


    /****************************************************************************
     *                           constructor                                    *
     ****************************************************************************/

    static {
	/* init the unsafe */
	try {
	    Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
	    field.setAccessible(true);
	    unsafe = (sun.misc.Unsafe) field.get(null);
	} catch (IllegalAccessException | IllegalArgumentException | 
		 NoSuchFieldException | SecurityException e) {            
	    throw new AssertionError(e);
	}

	try {
            next_addr = unsafe.objectFieldOffset
                (FFPAnsNode.class.getDeclaredField("next"));
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }

	base = unsafe.arrayBaseOffset(Object[].class);
	scale  = unsafe.arrayIndexScale(Object[].class); 	
    }      
    public FFPHashMap (){
	SHIFT_SIZE = 5;
	BASE_HASH_BUCKETS = 1 << SHIFT_SIZE;
	HN = newAtomicReferenceHash(null);
    }

    public FFPHashMap (int shiftSize) {
	SHIFT_SIZE = shiftSize;
	BASE_HASH_BUCKETS = 1 << SHIFT_SIZE;
	HN = newAtomicReferenceHash(null);
    }


    /***************************************************************************
     *                           private auxiliary methods                     *
     ***************************************************************************/
    
    private boolean IS_EQUAL_ENTRY(FFPAnsNode <E,V> node, int h, E t) {
	return node.equals(h, t);
    }

    private boolean IS_VALID_ENTRY(FFPAnsNode <E,V> node) {
	return node.valid();
    }
    
    private boolean IS_HASH(Object node){
	return (node instanceof Object[]);	
    }
    
    private boolean IS_EMPTY_BUCKET(Object[] curr_hash, 
				    int bucket_pos) {
	return curr_hash[bucket_pos] == curr_hash;
    }
    
    private boolean WAS_MARKED_AS_INVALID_NOW(FFPAnsNode <E,V> node) {
	return node.markAsInvalid();
    }

    private int HASH_ENTRY(int hash, int n_shifts) {
	return (int)  (((hash >> (SHIFT_SIZE * n_shifts)) & 
			((BASE_HASH_BUCKETS - 1))));
    }


    /****************************************************************************
     *                           check (search) and insert operation            *
     ****************************************************************************/

    private void insert_bucket_chain(Object[] curr_hash, 
				     FFPAnsNode <E,V> chain_node, 
				     Object insert_point_candidate,
				     Object insert_point_candidate_next,
				     FFPAnsNode <E,V> adjust_node, 
				     int n_shifts, 
				     int count_nodes) {

	int h = adjust_node.hash;
	int cn = count_nodes;

	Object ipc = insert_point_candidate;
	Object ipc_next = insert_point_candidate_next;
	Object chain_next;
	if (IS_VALID_ENTRY(chain_node)) {
	    cn++;
	    ipc = chain_node;
	    ipc_next = chain_node.getNext();
	    chain_next = ipc_next;
	} else
	    chain_next = chain_node.getNext();

	if (!IS_HASH(chain_next)) {
	    insert_bucket_chain(curr_hash, (FFPAnsNode <E,V>) chain_next, 
				ipc, ipc_next, adjust_node, n_shifts, cn);
	    return;
	}
	
	if ((Object[]) chain_next == curr_hash) {
	    if(cn == MAX_NODES_PER_BUCKET) {
		Object[] new_hash = newAtomicReferenceHash(curr_hash);		
		if (FFPAnsNode.class.cast(ipc).
		                 compareAndSetNext(ipc_next, new_hash)) {
		    int bucket = HASH_ENTRY(h, n_shifts);
		    adjust_chain_nodes(new_hash, curr_hash[bucket], n_shifts);  
		    curr_hash[bucket] = new_hash;
		    insert_bucket_array(new_hash, adjust_node, (n_shifts + 1));
		    return;
		} else
		    new_hash = null;
	    }
	    if (ipc == curr_hash) {
		/* ipc is a hash */
		int bucket = HASH_ENTRY(h, n_shifts);
		chain_next = curr_hash[bucket]; 
		if (chain_next == ipc_next) {
		    adjust_node.forceCompareAndSetNext(curr_hash);
		    if (compareAndSet(curr_hash, bucket, ipc_next, adjust_node)) {
			if (!IS_VALID_ENTRY(adjust_node))
			    /* 1) Adjusted a node that was valid before
			       and is invalid now. It might not have
			       been seen by the thread that was
			       deleting the node, thus we must check
			       is the node is present in the current
			       chain and if it is, then I must remove
			       it. At this stage we don't care about
			       the return of
			       check_delete_bucket_chain, since the
			       node was already returned by the thread
			       that was deleting the node */
			    delete_bucket_chain(curr_hash, adjust_node, n_shifts);
			return;
		    }
		    chain_next = curr_hash[bucket]; 
		}
		
		/* recover always to a hash */

		if (IS_HASH(chain_next))
		    if(chain_next != curr_hash) {
			/* invariant */
			insert_bucket_array((Object[]) chain_next, adjust_node, 
					    (n_shifts + 1));
			return;
		    }
		
		insert_bucket_array(curr_hash, 
				    adjust_node, n_shifts);
		return;
	    } else {
		/* ipc is a node */
		chain_next = FFPAnsNode.class.cast(ipc).getNext();
		if (chain_next == ipc_next) {
		    adjust_node.forceCompareAndSetNext(curr_hash);
		    if (FFPAnsNode.class.cast(ipc).
			compareAndSetNext(ipc_next, adjust_node)) {
			if (!IS_VALID_ENTRY(adjust_node))
			    /* same as comment in 1) */
			    delete_bucket_chain(curr_hash, adjust_node, n_shifts);    
			return;
		    }
		    chain_next = FFPAnsNode.class.cast(ipc).getNext();		
		}

		/* recover to a hash */
		if (!IS_HASH(chain_next) || (Object[]) chain_next == curr_hash) {
		    insert_bucket_array(curr_hash, adjust_node, n_shifts);
		    return;
		}
		/* recover with jump_hash */
	    }	    
	}

   	/* avoiding the temporary cicles in hashes/nodes */
	Object [] jump_hash = jump_prev_hash ((Object []) chain_next, curr_hash);
	if (jump_hash != null)
	    insert_bucket_array(jump_hash, adjust_node, (n_shifts + 1)); 
	return;
    }
    

    private void insert_bucket_array(Object [] curr_hash, 
				     FFPAnsNode <E,V> chain_node, 
				     int n_shifts) {	
	
	chain_node.forceCompareAndSetNext(curr_hash);
	if (!IS_VALID_ENTRY(chain_node))
	    return;
	
        int bucket;
        bucket = HASH_ENTRY(chain_node.hash, n_shifts);
        if (IS_EMPTY_BUCKET(curr_hash, bucket)) {
	    if (compareAndSet(curr_hash, bucket, curr_hash, chain_node)) {
		if (!IS_VALID_ENTRY(chain_node))
		    /* same as comment in 1) */
		    delete_bucket_chain(curr_hash, chain_node, n_shifts);
		return;
	    }
	}
        Object bucket_next = curr_hash[bucket];
        if (IS_HASH(bucket_next)) {
	    /* with the delete operation an entry might refer more
	     * than once to curr_hash */
	    int ns = n_shifts;
	    if ((Object []) bucket_next != curr_hash)
		ns++;
	    insert_bucket_array((Object []) bucket_next, chain_node, ns);
	    return;
	} else {
            insert_bucket_chain(curr_hash, (FFPAnsNode<E,V>) bucket_next,
				curr_hash, (FFPAnsNode<E,V>) bucket_next,
				chain_node, n_shifts, 0);
	    return;
	}
    }

    private void adjust_chain_nodes(Object [] new_hash,
				    Object chain_curr,
                                    int n_shifts) {
	/* the key idea is that at the begining of the expansion we
	   can always see the last node in the chain where the
	   expansion is applied.  We use this fact to create a chain
	   of nodes from the first node to the last node and store the
	   chain using the recursion. Then we use the
	   insert_bucket_array function on all nodes in the chain */
   
        if(chain_curr == (Object) new_hash)
            return;
        adjust_chain_nodes(new_hash, FFPAnsNode.class.cast(chain_curr).getNext(), 
			   n_shifts);
	insert_bucket_array(new_hash, FFPAnsNode.class.cast(chain_curr), n_shifts + 1);
	return;
    }

    private FFPAnsNode <E,V> check_insert_bucket_chain(Object [] curr_hash,
							 FFPAnsNode<E,V> chain_node, 
							 Object insert_point_candidate,
							 Object insert_point_candidate_next,
							 int h,
							 E t, 
							 V v, 
							 int n_shifts, 
							 int count_nodes) {

	int cn = count_nodes;
	if (IS_EQUAL_ENTRY(chain_node, h, t))
	    return chain_node;
	Object ipc = insert_point_candidate;
	Object ipc_next = insert_point_candidate_next;
	Object chain_next;
	if (IS_VALID_ENTRY(chain_node)) {
	    cn++;
	    ipc = chain_node;
	    ipc_next = chain_node.getNext();
	    chain_next = ipc_next;
	} else {
	    chain_next = chain_node.getNext();
	}

        if (!IS_HASH(chain_next)) {
            return check_insert_bucket_chain(curr_hash, (FFPAnsNode<E,V>) chain_next,
					     ipc, ipc_next, h, t, v, n_shifts, cn);
	}
	    
	if ((Object []) chain_next == curr_hash) {
            if(cn == MAX_NODES_PER_BUCKET) {
                Object [] new_hash =  newAtomicReferenceHash(curr_hash);
		/* ipc == chain_node (later you can do an assert of this where) */
                if (FFPAnsNode.class.cast(ipc).
   		                 compareAndSetNext(ipc_next, new_hash)) {		    
		    int bucket = HASH_ENTRY(h, n_shifts);
                    adjust_chain_nodes(new_hash, curr_hash[bucket], n_shifts);
		    curr_hash[bucket] = new_hash; 
                    return check_insert_bucket_array(new_hash, h, t, v, (n_shifts + 1));
                } else
                    new_hash = null;
            } 
	    if (ipc == curr_hash) {
		/* ipc is a hash */
		int bucket = HASH_ENTRY(h, n_shifts);
		chain_next = curr_hash[bucket]; 
		if (chain_next == ipc_next) {
		    FFPAnsNode <E,V> new_node = 
			new FFPAnsNode <E,V> (h, t, v, curr_hash);
		    if (compareAndSet(curr_hash, bucket, ipc_next, new_node))
			return new_node;
		    chain_next = curr_hash[bucket]; 
		}

		/* recover always to a hash bucket array */
		if (IS_HASH(chain_next))
		    if(chain_next != curr_hash)
			/* invariant */
			return check_insert_bucket_array((Object [])
							 chain_next, h, t, v, 
							 (n_shifts + 1));		    
		return check_insert_bucket_array(curr_hash, 
						 h, t, v, n_shifts);
	    } else {
		/* ipc is a node */
		chain_next = FFPAnsNode.class.cast(ipc).getNext();
		if (chain_next == ipc_next) {
		    FFPAnsNode <E,V> new_node = new 
			FFPAnsNode <E,V> (h, t, v, curr_hash);

		    if (FFPAnsNode.class.cast(ipc).
			compareAndSetNext(ipc_next, new_node))
			return new_node;
		    chain_next = FFPAnsNode.class.cast(ipc).getNext();		
		}
		
		/* recover always to a hash */
		if (!IS_HASH(chain_next) || (Object[]) chain_next == curr_hash)
		    return check_insert_bucket_array(curr_hash, h, t, v, 
						     n_shifts);
		/* recover with jump_hash */		
	    }	    
	}
	
	/* avoiding the temporary cicles in hashes/nodes */
	Object [] jump_hash = jump_prev_hash ((Object []) chain_next, curr_hash);
	if (jump_hash != null)
	    return check_insert_bucket_array(jump_hash, h, t, v, (n_shifts + 1));      
	return null;
    }

    private FFPAnsNode<E,V> check_insert_bucket_array(Object [] curr_hash, 
							int h, 
							E t, 
							V v, 
							int n_shifts) {
        int bucket;
        bucket = HASH_ENTRY(h, n_shifts);
        if (IS_EMPTY_BUCKET(curr_hash, bucket)) {
		FFPAnsNode <E,V> new_node = new 
		    FFPAnsNode <E,V> (h, t, v, curr_hash);
		if (compareAndSet(curr_hash, bucket, curr_hash, new_node)) {
		    return new_node;
		}
        }
        Object bucket_next = curr_hash[bucket];
        if (IS_HASH(bucket_next)) {
	    int ns = n_shifts;
	    if ((Object []) bucket_next != curr_hash)
		ns++;
	    return check_insert_bucket_array((Object []) bucket_next, 
					     h, t, v, ns);
        } else
            return check_insert_bucket_chain(curr_hash, 
					     (FFPAnsNode<E,V>) bucket_next,
					     curr_hash, 
					     (FFPAnsNode<E,V>) bucket_next,
					     h, t, v, n_shifts, 0);           
    }
    
    /****************************************************************************
     *                           check (search) operation                       *
     ****************************************************************************/
    

    private FFPAnsNode<E,V> check_bucket_array(Object [] curr_hash, 
						 int h, 
						 E t,
						 int n_shifts) {
	int ns = n_shifts;

        do {
            int bucket;
            bucket = HASH_ENTRY(h, ns);
	    Object bucket_next = curr_hash[bucket];
            if (bucket_next == curr_hash)
                return null;
            
            if (IS_HASH(bucket_next)) {
		ns++;
		curr_hash = (Object[]) bucket_next;		
            } else
		return check_bucket_chain(curr_hash, 
					  (FFPAnsNode<E,V>) bucket_next,
					  curr_hash, 
					  (FFPAnsNode<E,V>) bucket_next,
					  h, t, ns);	    
        } while(true);
    }

    
    private FFPAnsNode <E,V> check_bucket_chain(Object [] curr_hash, 
    					    FFPAnsNode <E,V> chain_node, 
    					    Object insert_point_candidate,
    					    Object insert_point_candidate_next,
                                            int h, 
    					    E t, 
    					    int n_shifts) {
    	if (IS_EQUAL_ENTRY(chain_node, h, t))
            return chain_node;       
	
    	Object ipc = insert_point_candidate;
    	Object ipc_next = insert_point_candidate_next;
    	Object chain_next;
    	if (IS_VALID_ENTRY(chain_node)) {
    	    ipc = chain_node;
    	    ipc_next = chain_node.getNext();
    	    chain_next = ipc_next;
    	} else
    	    chain_next = chain_node.getNext();

    	if (!IS_HASH(chain_next))
            return check_bucket_chain(curr_hash, (FFPAnsNode<E,V>) chain_next, 
    				      ipc, ipc_next, h, t, n_shifts);
        
    	if ((Object []) chain_next == curr_hash) {

	    if (ipc == curr_hash) {
		/* ipc is a hash */
		int bucket = HASH_ENTRY(h, n_shifts);
		chain_next = curr_hash[bucket]; 
		if (chain_next == ipc_next)
		    return null;
		
		/* recover always to a hash */

		if (IS_HASH(chain_next))
		    if(chain_next != curr_hash)
			/* invariant */
			return check_bucket_array((Object [])
						  chain_next, h, t,
						  (n_shifts + 1));		
		return check_bucket_array(curr_hash, 
					  h, t, n_shifts);
	    } else {
		/* ipc is a node */
		chain_next = FFPAnsNode.class.cast(ipc).getNext();
		if (chain_next == ipc_next) 
		    return null;

		/* recover always to a hash */
		if (!IS_HASH(chain_next) || (Object[]) chain_next == curr_hash)
		    return check_bucket_array(curr_hash, 
					      h, t, n_shifts);
		/* recover with jump_hash */			
	    }	    
	}

	/* avoiding the temporary cicles in hashes/nodes */
	Object [] jump_hash = jump_prev_hash ((Object []) chain_next, curr_hash);
	if (jump_hash != null)
	    return check_bucket_array(jump_hash, h, t, (n_shifts + 1));
	return null;
    }     


    /****************************************************************************
     *                           check (search) delete operation                *
     ****************************************************************************/

    private FFPAnsNode<E,V> check_delete_bucket_array(Object [] curr_hash, 
							int h,
							E t,
							int n_shifts) {
        int bucket;
        bucket = HASH_ENTRY(h, n_shifts);
        if (IS_EMPTY_BUCKET(curr_hash, bucket))
            return null;
        Object bucket_next = curr_hash[bucket];
        if (IS_HASH(bucket_next)) {
	    int ns = n_shifts;
	    if ((Object []) bucket_next != curr_hash)
    		ns++;	    
            return check_delete_bucket_array((Object []) bucket_next,
    					     h, t, ns);
	} else
            return check_delete_bucket_chain(curr_hash, 
					     (FFPAnsNode<E,V>) bucket_next, 
    					     curr_hash, 
					     (FFPAnsNode<E,V>) bucket_next,
    					     h, t, n_shifts);
    }

    private FFPAnsNode<E,V> check_delete_bucket_chain(Object [] curr_hash, 
    						   FFPAnsNode<E,V> chain_node, 
    						   Object insert_point_candidate,
    						   Object insert_point_candidate_next,
                                                   int h, 
    						   E t, 
    						   int n_shifts) {
    	if (IS_EQUAL_ENTRY(chain_node, h, t)) {
    	    /* be aware that at this instant the chain_node is seen as valid */
    	    if (WAS_MARKED_AS_INVALID_NOW(chain_node)) {
    		delete_bucket_chain(curr_hash, chain_node, n_shifts);
    		return chain_node;
	    }
    	}
	
    	Object ipc = insert_point_candidate;
    	Object ipc_next = insert_point_candidate_next;
    	Object chain_next;
    	if (IS_VALID_ENTRY(chain_node)) {
    	    ipc = chain_node;
    	    ipc_next = chain_node.getNext();
    	    chain_next = ipc_next;
    	} else
    	    chain_next = chain_node.getNext();

    	if (!IS_HASH(chain_next)) {
            return check_delete_bucket_chain(curr_hash, 
					     (FFPAnsNode<E,V>) chain_next, 
    					     ipc, ipc_next, h, t, n_shifts);
	}
        
    	if ((Object []) chain_next == curr_hash) {
	    if (ipc == curr_hash) {
		/* ipc is a hash */
		int bucket = HASH_ENTRY(h, n_shifts);
		chain_next = curr_hash[bucket]; 
		if (chain_next == ipc_next)
		    return null;
		
		/* recover always to a hash bucket array */
		if (IS_HASH(chain_next))
		    if(chain_next != curr_hash)
			/* invariant */
			return check_delete_bucket_array((Object [])
							 chain_next, h, t,
							 (n_shifts + 1));		
		return check_delete_bucket_array(curr_hash, 
						 h, t, n_shifts);		
	    } else {
		/* ipc is a node */
		chain_next = FFPAnsNode.class.cast(ipc).getNext();
		if (chain_next == ipc_next) 
		    return null;
		
		/* recover always to a hash bucket array */
		if (!IS_HASH(chain_next) || (Object[]) chain_next == curr_hash)
		    return check_delete_bucket_array(curr_hash, 
						     h, t, n_shifts);		
		/* recover with jump_hash */
	    }	    
	}
	/* avoiding the temporary cicles in hashes/nodes */
	Object [] jump_hash = jump_prev_hash ((Object []) chain_next, curr_hash);
	if (jump_hash != null)
	    return check_delete_bucket_array(jump_hash, h, t, (n_shifts + 1));
	return null;
    }     

    private FFPAnsNode<E,V> delete_bucket_chain(Object [] curr_hash, 
						  FFPAnsNode chain_node, 
						  int n_shifts) {
    	do {
    	    Object chain_next_valid_candidate;
    	    Object chain_curr = (Object) chain_node;
    	    /* get chain_next_valid - (begin) */  
    	    do
    		chain_curr = FFPAnsNode.class.cast(chain_curr).getNext();
    	    while (!IS_HASH(chain_curr) && 
		   !FFPAnsNode.class.cast(chain_curr).valid());
	    
    	    if (IS_HASH(chain_curr) && ((Object [])chain_curr != curr_hash)) {
    		/* re-positioning the thread in next hash level. The
    		   pointer in the chain of curr_hash will be corrected
    		   by the adjust_chain_nodes procedure */
		Object [] jump_hash = jump_prev_hash ((Object []) chain_curr, curr_hash);
		if (jump_hash != null)
		    return delete_bucket_chain(jump_hash, chain_node, (n_shifts + 1));
		return null;
    	    }
	    

    	    /* chain_curr is a valid node or the curr_hash */
    	    chain_next_valid_candidate = chain_curr;
    	    if (!IS_HASH(chain_curr))
    		do 
    		    chain_curr = FFPAnsNode.class.cast(chain_curr).getNext();
    		while (!IS_HASH(chain_curr));	    
    	    if (chain_curr != curr_hash) {
    		/* re-positioning the thread in next hash level.  the
    		   pointer in the chain of curr_hash will be corrected
    		   by the adjust_chain_nodes procedure */
		Object [] jump_hash = jump_prev_hash ((Object []) chain_curr, curr_hash);
		if (jump_hash != null)
		    return delete_bucket_chain(jump_hash, chain_node, (n_shifts + 1));
		return null;
    	    }

    	    Object chain_prev_valid_candidate = curr_hash;
    	    int bucket = HASH_ENTRY(chain_node.hash, n_shifts);
    	    chain_curr = curr_hash[bucket];
    	    Object chain_prev_valid_candidate_next = chain_curr;

    	    while (!IS_HASH(chain_curr) && 
    		   FFPAnsNode.class.cast(chain_curr) != chain_node) {
		
    		if (FFPAnsNode.class.cast(chain_curr).valid()) {
    		    chain_prev_valid_candidate = chain_curr;
		    chain_curr = FFPAnsNode.class.cast(chain_curr).getNext();
    		    chain_prev_valid_candidate_next = chain_curr;
    		} else		    
		    chain_curr = FFPAnsNode.class.cast(chain_curr).getNext();
    	    }

    	    if (IS_HASH(chain_curr)) {

		if((Object [])chain_curr == curr_hash)
		    /* unable to find chain_node in the chain */
		    return null;
		else {
		    Object [] jump_hash = jump_prev_hash ((Object []) chain_curr, curr_hash);
		    if (jump_hash != null)
			return delete_bucket_chain(jump_hash, chain_node, (n_shifts + 1));
		    return null;
		}		    
    	    } else /* FFPAnsNode.class.cast(chain_curr) == chain_node */ {
		if (chain_prev_valid_candidate == curr_hash) {
    		    if (compareAndSet(curr_hash, bucket, 
				      chain_prev_valid_candidate_next, 
				      chain_next_valid_candidate)) {
    			/* update was ok */
			if (!IS_HASH(chain_next_valid_candidate) && 
			    !IS_VALID_ENTRY((FFPAnsNode)chain_next_valid_candidate))
			    /* restart the process */
			    continue;
    			return chain_node;
			
    		    } else /* compareAndSet == false */ {
    			/* restart the process */
    			continue;
    		    }
    		} else /* chain_prev_valid_candidate is node */ {
    		    if (FFPAnsNode.class.cast(chain_prev_valid_candidate).
    			compareAndSetNext(chain_prev_valid_candidate_next, 
    					  chain_next_valid_candidate)) {
    			/* update was ok */
						
			if (!IS_HASH(chain_next_valid_candidate) && 
			    !IS_VALID_ENTRY((FFPAnsNode)chain_next_valid_candidate))
			    /* restart the process */
			    continue;
    			return chain_node; 
    		    } else /* compareAndSetNext == false */ {
    			/* restart the process */
    			continue;
    		    }		    
    		}
    	    }
    	} while(true);
    }

   
    /****************************************************************************
     *                           flush statistics                               *
     *                           (non concurrent)                               *
     ****************************************************************************/
    
    private void flush_bucket_chain(Object chain_node, 
    				    int count_nodes,
    				    int level,
    				    boolean flush_nodes) {
    	if (IS_HASH(chain_node)) {
    	    if (count_nodes > total_max_nodes)
    		total_max_nodes = count_nodes;
    	    if (count_nodes < total_min_nodes)
    		total_min_nodes = count_nodes;
    	    return;
    	}
    	if (IS_VALID_ENTRY(FFPAnsNode.class.cast(chain_node)))
    	    total_nodes_valid++;
    	else
    	    total_nodes_invalid++;
    	if (flush_nodes)
    	    System.err.println(" " + FFPAnsNode.class.cast(chain_node).entry + " ");
    	flush_bucket_chain(FFPAnsNode.class.cast(chain_node).getNext(), 
    			   count_nodes + 1, level, flush_nodes);
    	return;
    }
    
    private void flush_bucket_array(Object[] curr_hash, 
    				    int level, 
    				    boolean flush_nodes) {
    	int bucket_entry = 0;
    	do {
            if (flush_nodes)
    		System.err.println("\n bkt entry -> " + 
    				   bucket_entry + " (level = " + 
    				   level + ")");
            total_buckets++;

            if (!IS_EMPTY_BUCKET(curr_hash, bucket_entry)) { 
    		Object bucket_next = curr_hash[bucket_entry];
    		if (IS_HASH(bucket_next)) 
    		    flush_bucket_array((Object[]) bucket_next, 
				       level + 1, flush_nodes);		
    		else {
    		    flush_bucket_chain(bucket_next, 0, level, flush_nodes);
    		    /* leaf bucket_array */
    		    if (level > total_max_hash_trie_depth)
    			total_max_hash_trie_depth = level;
    		    if (level < total_min_hash_trie_depth)
    			total_min_hash_trie_depth = level;
    		}
    		if (flush_nodes)
    		    System.err.println("");
            } else {
                total_empties++;
    		/* leaf bucket_array */
    		if (level > total_max_hash_trie_depth)
    		    total_max_hash_trie_depth = level;		
    		if (level < total_min_hash_trie_depth)
    		    total_min_hash_trie_depth = level;
    	    }

    	} while (++bucket_entry < BASE_HASH_BUCKETS);
    	return;
    }
    
    public void flush_hash_statistics(boolean flush_nodes) {
	
    	total_nodes_valid = 0;
    	total_nodes_invalid = 0;
    	total_buckets = 0; 
    	total_empties = 0; 
    	total_min_hash_trie_depth = Long.MAX_VALUE;
    	total_max_hash_trie_depth = 0;
    	total_max_nodes = 0;
    	total_min_nodes =  Long.MAX_VALUE;

    	flush_bucket_array(HN, 0, flush_nodes);
    	if (total_min_nodes ==  Long.MAX_VALUE)
    	    total_min_nodes = 0;

    	System.err.println("-----------------------------------------------------");
    	System.err.println("  Nr of valid nodes     = " + total_nodes_valid);
    	System.err.println("  Nr of invalid nodes   = " + total_nodes_invalid);
    	System.err.println("  Nr of buckets         = " + total_buckets);
    	System.err.println("  Nr of empty buckets   = " + total_empties);
    	System.err.println("  Min hash trie depth = " + total_min_hash_trie_depth + 
			   "   (Root depth = 0)");
    	System.err.println("  Max hash trie depth = " + total_max_hash_trie_depth + 
			   "   (Root depth = 0)");
    	System.err.println("  Max nodes (non empty) = " + total_max_nodes + 
    			   "   (MAX_NODES_PER_BUCKET = " + MAX_NODES_PER_BUCKET + ")");
    	System.err.println("  Min nodes (non empty) = " + total_min_nodes);
    	long non_empty_buckets = total_buckets - total_empties;
    	if (non_empty_buckets == 0)
    	    System.err.println("  Avg nodes per bucket (valid + invalid) = " + 
    			       (float)(total_nodes_valid + total_nodes_invalid) / 
    			       (total_buckets) + " (non empty only) = 0.0");
    	else
    	    System.err.println("  Avg nodes per bucket (valid + invalid) = " + 
    			       (float)(total_nodes_valid + total_nodes_invalid) / 
    			       (total_buckets) + " (non empty only) = " + 
    			       (float) (total_nodes_valid + total_nodes_invalid) / 
    			       (float) non_empty_buckets);

    	System.err.println("-----------------------------------------------------");
    }

    public int hash (E k) {
	int h = k.hashCode ();	
	return h;
    }
    
    /****************************************************************************
     *                            API compatibility                             *
     *            (Java concurrent data structures - CHM and Skiplists)         *
     ****************************************************************************/

    public long size() {
	total_nodes_valid = 0;	
	total_nodes_invalid = 0;
	flush_bucket_array(HN, 0, false);
	if (total_nodes_invalid != 0) {
	    System.out.println("ERROR INVALID NODES VISIBLE -> " + total_nodes_invalid);
	    System.exit(0);
	}
	return total_nodes_valid;
    }
    
    public V get(E t) {
	int h = hash(t);
	FFPAnsNode <E,V> node = check_bucket_array(HN, h, t, 0);
	if (node != null)
	    return node.value;
	return null;       
    }
    
    public void put(E t, V v) {
	int h = hash(t);
	check_insert_bucket_array(HN, h, t, v, 0);
	return;
    }

    public void remove(E t) {
	int h = hash(t);
	check_delete_bucket_array(HN, h, t, 0);
	return;
    }

    public V replace(E t, V v) {
	/* it is not atomic for the moment */
	int h = hash(t);
	FFPAnsNode <E,V> node = check_bucket_array(HN, h, t, 0);
	if (node == null)
	    return null;
	V prev_value = node.value;
	node.value = v;	
	return prev_value;
    }

    public boolean isEmpty() {
	if (this.size() == 0)
	    return true;
	return false;
    }

    public void clear() {
	HN = newAtomicReferenceHash(null);
	return;
    }




    /****************************************************************************
     *                         FFPAnsNode                                       *
     ****************************************************************************/

    
    static class FFPAnsNode <E, V> {

	static class Pair {
	    final Object reference;
	    final boolean mark;
	    Pair(Object reference, boolean mark) {
		this.reference = reference;
		this.mark = mark;
	    }
	}

	public final int hash;
	public final E entry;
	public V value;
	private Pair next;
	
	public FFPAnsNode (int h, E e, V v, Object node_next) {
	    this.hash = h;
	    this.entry = e;
	    this.value = v;
	    this.next = new Pair(node_next, true);
	}
	
	public Object getNext() {
	    return next.reference;
	}
	
	public boolean valid() {
	    return next.mark;
	}
	
	public boolean compareAndSetNext(Object expect, Object update) {	    
	    return compareAndSet(expect, update, true, true);
	}
	
	private boolean compareAndSet(Object expectedReference,
				      Object newReference,
				      boolean expectedMark,
				      boolean newMark) {
	    Pair current_pair = next;
	    return
		expectedReference == current_pair.reference &&
		expectedMark == current_pair.mark &&
		((newReference == current_pair.reference &&
		  newMark == current_pair.mark) ||
		 unsafe.compareAndSwapObject(this, next_addr, 
					     current_pair, 
					     (new Pair(newReference, newMark))));
	}

	public void forceCompareAndSetNext(Object update) {
	    /* force CAS but keep the state unchanged */
	    Object node_next = next.reference;
	    if (node_next == update)
		return;	
	    boolean state = next.mark;
	    
	    while (!compareAndSet(node_next, update, state, state)) {
		state = next.mark;
		node_next = next.reference;
	    }
	    return;
	}
	
	public boolean equals(int h, E t) {	
	    if (h == hash && 
		entry.equals(t) && valid())
		return true;
	    return false;
	}
	
	public boolean markAsInvalid() {	
	    Object node_next = next.reference;	    
	    while (!compareAndSet(node_next, node_next, true, false)) {
		if (!valid())
		    return false;
		node_next = next.reference;
	    }
	    return true;
	}    
    }

    /****************************************************************************
     *                         FFPAtomicReferenceArray                          *
     ****************************************************************************/

    private Object [] newAtomicReferenceHash(Object ph) {    	
	Object [] hash = new Object[BASE_HASH_BUCKETS + 1];        
	for (int i = 0; i < BASE_HASH_BUCKETS; i++)
	    hash[i] = hash;
	hash[BASE_HASH_BUCKETS] = ph;
	return hash;
    }
    
    private final boolean compareAndSet(Object[] hash, int i, Object expect, Object update) {
	long raw_index = base + i * scale;
	return unsafe.compareAndSwapObject(hash, raw_index, expect, update);
    }    

    private final Object [] jump_prev_hash(Object [] curr_hash, 
					   Object [] stop_hash) {
	    
	Object []  jump_hash, prev_hash;
	jump_hash =  curr_hash;
	prev_hash = (Object []) curr_hash[BASE_HASH_BUCKETS];
	while(prev_hash != null && prev_hash != stop_hash) {
	    jump_hash = prev_hash;
	    prev_hash = (Object []) jump_hash[BASE_HASH_BUCKETS];
	}
	if (prev_hash == null)
	    return null;
	else
	    return jump_hash;
    }
}






