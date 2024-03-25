#include "per_thread.h"

__thread int thread_id;
__thread struct tls_context *tls_context;

#define alloc_split_record(high)	\
int alloc_##high##_split_record(	\
				    uintptr_t *split_addr, u64 hashed_key,	\
				    void *bucket, int l_depth, int linking, \
                    struct split_record *head_record,   \
				    struct split_record **new_record) {	\
    int ret;    \
                                                                                        \
	if (linking)	\
		prefech_r0(head_record);	\
																						\
	ret = malloc_aligned((void **)new_record, SPLIT_RECORD_SIZE, SPLIT_RECORD_SIZE);	\
																						\
	if (unlikely(ret))	\
    	return -ret;	\
																						\
    (*new_record)->ent[0].hashed_key = hashed_key;	\
	(*new_record)->ent[0].bucket_ptr = set_split_entry_bucket_ptr(bucket, l_depth);	\
																						\
    if (!linking) {	\
    	(*new_record)->next_record = init_##high##_split_record(*new_record, thread_id, 1);	\
    	(*new_record)->prev_record = *new_record;	\
		release_fence();	\
    	WRITE_ONCE(*split_addr, set_first_split(*new_record, 1, 0));	\
        return 0;	\
    }	\
																						\
	return 1;	\
}


#define re_check_low_split_head(split_head) (!READ_ONCE(*split_head))
#define re_check_high_split_head(split_head) 0

#define append_split_ent(high)  \
void *append_##high##_split_ent(   \
                uintptr_t *split_head, u64 hashed_key,  \
                void *bucket, int l_depth) {    \
    uintptr_t split, lock_contex;    \
    struct split_record *new_record, *head_record;   \
    int ret, num, record_num, full; \
                                                                                                    \
retry_append_##high##_split_ent :   \
    split = READ_ONCE(*split_head); \
    head_record = (struct split_record *)first_split_addr(split);   \
    num = first_split_idx(split);   \
    record_num = split_record_num(split);   \
                                                                                                    \
	full = (num == SPLIT_ENT_PER_RECORD);   \
                                                                                                    \
    if (!split || full) {   \
		ret = alloc_##high##_split_record(split_head, hashed_key, bucket,   \
                                l_depth, full, head_record, &new_record);   \
                                                                                                    \
        if (ret == 0)   \
            return new_record;  \
                                                                                                    \
		if (ret != 1)   \
			return NULL; \
    }   \
                                                                                                    \
    if (unlikely(!(lock_contex = lock_##high##_split_record(head_record))) ||  \
            re_check_##high##_split_head(split_head)) { \
        if (full)   \
            free(new_record);   \
                                                                                                    \
		spin_fence();   \
		goto retry_append_##high##_split_ent;    \
	}   \
                                                                                                    \
    if (full) {   \
        add_new_##high##_split_record(split_head, record_num, head_record, lock_contex, new_record);    \
        return new_record;  \
    } else {    \
        update_unlock_split_record(head_record, lock_contex, num, record_num,   \
                                        split_head, hashed_key, bucket, l_depth);   \
        return head_record; \
    }   \
}

__attribute__((always_inline))
void update_unlock_split_record(struct split_record *head, uintptr_t head_contex,
                                int idx_num, int rec_num, uintptr_t *split_head, 
                                    u64 hashed_key, void *bucket, int l_depth) {
    uintptr_t split, r;
    head->ent[idx_num].hashed_key = hashed_key;
    head->ent[idx_num].bucket_ptr = set_split_entry_bucket_ptr(bucket, l_depth);
                                                                                    
    split = set_first_split(head, ++idx_num, rec_num);
    WRITE_ONCE(*split_head, split);
                                                                                    
    r = inc_split_record_ent_idx(head_contex);
    unlock_split_record(head, r);
}


__attribute__((always_inline))
static void unlock_split_record_via_new(struct split_record *record, 
                                            struct split_record *new) {
    uintptr_t r = init_low_split_record(new, INVALID_THREAD_ID, SPLIT_ENT_PER_RECORD);
    release_fence();
    WRITE_ONCE(record->next_record, r);
}

__attribute__((always_inline))
static void add_new_low_split_record(uintptr_t *split_head, int rec_num,
                                        struct split_record *head, 
                                        uintptr_t head_contex,
                                        struct split_record *new_record) {
    u64 split;
    struct split_record *next = (struct split_record *)split_record_addr(head_contex);

    new_record->next_record = reset_split_record_ent_idx(head_contex); //init_low_split_record(next, tid, 1);
    new_record->prev_record = head;

    release_fence();
    WRITE_ONCE(next->prev_record, new_record);

    split = set_first_split(new_record, 1, rec_num + 1);
    WRITE_ONCE(*split_head, split);

    unlock_split_record_via_new(head, new_record);
}

__attribute__((always_inline))
static void add_new_high_split_record(uintptr_t *split_head, int rec_num,
                                    struct split_record *head, 
                                    uintptr_t head_contex,
                                    struct split_record *new_record) {
    u64 split;
    struct split_record *prev = head->prev_record;

    new_record->next_record = init_high_split_record(head, 0, 1);
    new_record->prev_record = prev;

    head->prev_record = new_record;

    release_fence();
    WRITE_ONCE(prev->next_record, 
        set_locked_split_record_addr(prev->next_record, new_record));
    split = set_first_split(new_record, 1, rec_num + 1);

    release_fence();
    WRITE_ONCE(*split_head, split);
}


__attribute__((always_inline))	static alloc_split_record(high);
__attribute__((always_inline))	static alloc_split_record(low);


append_split_ent(high);
append_split_ent(low);

__attribute__((always_inline))
static uintptr_t upgrade_lock_split_record(struct split_record *record) {
    uintptr_t next_record = READ_ONCE(record->next_record);

    if (likely(!split_record_high(next_record))) {
        if (likely(!split_record_locked(next_record))) {
            uintptr_t new = set_split_record_high_locked(next_record);

            if (likely(cas_bool(&record->next_record, next_record, new)))
                return next_record;
        }

       return 0;
    }

    return 1;
}

__attribute__((always_inline))
static uintptr_t upgrade_lock_prev_split_record(struct split_record *record, 
                                    struct split_record *n_record, int tid) {
    struct split_record *p_record = READ_ONCE(record->prev_record);
    uintptr_t next_record = READ_ONCE(p_record->next_record);

    if (likely(!split_record_doing(next_record))) {
        if (likely(!split_record_locked_or_high(next_record))) {
            uintptr_t new;
            int t = split_record_tid(next_record), idx = split_record_ent_idx(next_record);

            new = ((t != INVALID_THREAD_ID) ? 
                    init_low_split_record(n_record, t, idx) : init_low_split_record(n_record, tid, idx));

            if (likely(cas_bool(&p_record->next_record, next_record, set_split_record_locked(new)))) {
                WRITE_ONCE(n_record->prev_record, p_record);
                return new;
            }
        }

        return 0;
    }

    return 1;
}

static void move_split_record_to_high_list(struct split_record *record, uintptr_t record_context) {
    struct split_record *head_record, *next_record;
    uintptr_t *split_head = &(tls_context_array[thread_id].prio_split_list.split_list[1]);
    uintptr_t split, head_context;
    int idx = split_record_ent_idx(record_context);

retry_move_split_record_to_high_list :
    split = READ_ONCE(*split_head);

    if (!split) {
        record_context = init_high_split_record(record, 0, idx);
        WRITE_ONCE(record->next_record, record_context);

        record->prev_record = record;

        split = set_first_split(record, idx, 0);

        release_fence();
        WRITE_ONCE(*split_head, split);
        return;
    }

    head_record = (struct split_record *)first_split_addr(split);

    if (unlikely(!(head_context = lock_high_split_record(head_record)))) {
        spin_fence();
        goto retry_move_split_record_to_high_list;
    }

    next_record = (struct split_record *)split_record_addr(head_context);
    record_context = init_high_split_record(next_record, 0, idx);
    WRITE_ONCE(record->next_record, record_context);

    record->prev_record = head_record;
    next_record->prev_record = record;

    head_context = set_split_record_addr(head_context, record);
    unlock_split_record(head_record, head_context);
}

void upgrade_split_record(struct split_record *record) {
    uintptr_t locked_context, prev_locked_context;
    uintptr_t *split_head;
    struct split_record *next_record;
    int tid;

retry_upgrade_split_record :
    locked_context = upgrade_lock_split_record(record);

    if (unlikely(locked_context == 1))
        return;

    if (unlikely(locked_context == 0)) {
        spin_fence();
        goto retry_upgrade_split_record;
    }

    tid = split_record_tid(locked_context);
    next_record = (struct split_record *)split_record_addr(locked_context);

    if (unlikely(tid != INVALID_THREAD_ID)) {
        split_head = &(tls_context_array[tid].prio_split_list.split_list[0]);

        if (next_record == record) {
            WRITE_ONCE(*split_head, 0);
            move_split_record_to_high_list(record, locked_context);
            return;
        }
    }

    prev_locked_context = upgrade_lock_prev_split_record(record, next_record, tid);

    if (unlikely(prev_locked_context <= 1)) {
        unlock_split_record(record, locked_context);

        if (prev_locked_context == 1)
            return;

        spin_fence();
        goto retry_upgrade_split_record;
    }

    if (unlikely(tid != INVALID_THREAD_ID)) {
        int rec_num = split_record_num(*split_head);
        uintptr_t split = set_first_split(record->prev_record, SPLIT_ENT_PER_RECORD, rec_num);
        
        WRITE_ONCE(*split_head, split);
    }

    unlock_split_record(record->prev_record, prev_locked_context);

    move_split_record_to_high_list(record, locked_context);
}


int reclaim_kv_to_rcpage(struct kv *kv) {
    struct reclaim_page *head, *page = tls_context->kv_rcpage;
    u64 index = page->index;

    page->entry[index] = kv;

    if (likely(++index != RECLAIM_PAGE_ENT_NUM))
        page->index = set_reclaim_page_index(index, NULL);
    else {
        tls_context->kv_rcpage = (struct reclaim_page *)
                        malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE);

        if (unlikely((void *)tls_context->kv_rcpage == MAP_FAILED)) {
            tls_context->kv_rcpage = page;
            return -1;
        }

        tls_context->kv_rcpage->index = 0;

        head = READ_ONCE(tls_context->kv_rclist_head);
        page->index = set_reclaim_page_index(index, head);

        if (!head || unlikely(!cas_bool(&tls_context->kv_rclist_head, head, page))) {
            release_fence();
            WRITE_ONCE(tls_context->kv_rclist_head, page);
            release_fence();
            WRITE_ONCE(tls_context->kv_rclist_tail, page);
        }
    }

    return 0;
}