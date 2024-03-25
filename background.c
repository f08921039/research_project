#include "background.h"
#include "ext_hash.h"

#define NUM_OF_EH_SEG_ALLOC 7

struct tls_context tls_context_array[THREAD_NUM];

int delay_to_rcpage(struct epoched_reclaim_context *reclaim_ctx, void *reclaim_ent) {
    struct reclaim_page *page, *tail = reclaim_ctx->rclist_tail;

    if (tail) {
        uintptr_t index = tail->index;

        if (likely(index != RECLAIM_PAGE_ENT_NUM)) {
            tail->entry[index++] = reclaim_ent;
            tail->index = index;
            return 0;
        }
    }

    page = (struct reclaim_page *)
                malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE);

    if (unlikely((void *)page == MAP_FAILED))
        return -1;

    page->entry[0] = reclaim_ent;
    page->index = set_reclaim_page_index(1, NULL);

    reclaim_ctx->rclist_tail = page;

    if (tail)
        tail->index = set_reclaim_page_index(RECLAIM_PAGE_ENT_NUM, page);
    else
        reclaim_ctx->rclist_head = page;

    return 0;
}


static void hook_kv_reclaim(struct background_context *bg_context, struct tls_context *context) {
    struct epoched_reclaim_context *chunk_reclaim = &bg_context->chunk_reclaim;
    struct reclaim_page *page = READ_ONCE(context->kv_rclist_tail);

    if (page) {
        struct reclaim_page *first_page;

        if (!chunk_reclaim->rclist_tail)
            chunk_reclaim->rclist_tail = page;
        
        release_fence();
        WRITE_ONCE(context->kv_rclist_tail, NULL);

        page->index = set_reclaim_page_index(RECLAIM_PAGE_ENT_NUM, chunk_reclaim->rclist_head);
        
        first_page = READ_ONCE(context->kv_rclist_head);

        while (1) {
            page = cas(&context->kv_rclist_head, first_page, NULL);

            if (likely(page == first_page))
                break;

            first_page = page;
        }
        
        chunk_reclaim->rclist_head = first_page;
    }
}

static int hook_high_split_record(struct background_context *bg_context, struct tls_context *context) {
    struct split_record *record_head = bg_context->split_list.prio_split_list[1];
    uintptr_t *split_target = &(context->prio_split_list.split_list[1]);
    uintptr_t split;
    struct split_record *target_first;
    int idx_num, record_num;

    while (1) {
        split = READ_ONCE(*split_target);

        if (!split)
            return 0;

        target_first = (struct split_record *)first_split_addr(split);

        if (likely(lock_high_split_record(target_first)))
            break;

        spin_fence();
    }

    idx_num = first_split_idx(split);
    record_num = split_record_num(split);

    WRITE_ONCE(*split_target, 0);

    if (!record_head)
        bg_context->split_list.prio_split_list[1] = target_first;
    else {
        struct split_record *record_tail = record_head->prev_record, *target_last = target_first->prev_record;
        uintptr_t record_contex;
        int idx;

        idx = split_record_ent_idx(record_tail->next_record);
        record_contex = init_high_locked_split_record(target_first, idx);
        WRITE_ONCE(record_tail->next_record, record_contex);

        idx = split_record_ent_idx(target_last->next_record);
        record_contex = init_high_locked_split_record(record_head, idx);
        WRITE_ONCE(target_last->next_record, record_contex);
        
        record_head->prev_record = target_last;
        target_first->prev_record = record_tail;
    }

    return record_num * SPLIT_ENT_PER_RECORD + idx_num;
}

__attribute__((always_inline))
static uintptr_t lock_first_low_split_record(struct split_record *record, 
                                                struct split_record *head) {
    uintptr_t next_record = READ_ONCE(record->next_record);
    int tid = split_record_tid(next_record);

    if (likely(!split_record_locked_or_high(next_record)) && likely(tid != INVALID_THREAD_ID)) {
        struct split_record *next = (struct split_record *)split_record_addr(next_record);
        int idx = split_record_ent_idx(next_record);
        uintptr_t new = (head) ? init_low_split_record(next, INVALID_THREAD_ID, idx) : 
                                        init_high_locked_doing_split_record(next, idx);
        int ret = cas_bool(&record->next_record, next_record, set_split_record_locked(new));

        if (likely(ret))
            return new;
    }

    return 0;
}

__attribute__((always_inline))
static uintptr_t locklink_last_low_split_record(struct split_record *last, 
                                    struct split_record *first, struct split_record *head) {
    uintptr_t next_record = READ_ONCE(last->next_record);

    if (first == last || likely(!split_record_locked_or_high(next_record))) {
        int idx = split_record_ent_idx(next_record);
        uintptr_t new = init_low_split_record(head, INVALID_THREAD_ID, idx);
        int ret = cas_bool(&last->next_record, next_record, set_split_record_locked(new));

        if (likely(ret))
            return new;
    }

    return 0;
}

__attribute__((always_inline))
static uintptr_t link_low_split_tail_with_target(struct split_record *head, 
                                struct split_record *tail, struct split_record *target) {
    uintptr_t next_record = READ_ONCE(tail->next_record);
    int idx = split_record_ent_idx(next_record);

    if (unlikely(head == tail)) {
        next_record = init_high_locked_doing_split_record(target, idx);
        release_fence();
        WRITE_ONCE(head->next_record, next_record);
        return 1;
    }

    if (likely(!split_record_locked_or_high(next_record))) {
        uintptr_t new = init_low_split_record(target, INVALID_THREAD_ID, idx);

        if (likely(cas_bool(&tail->next_record, next_record, new)))
            return 1;
    }

    return 0;
}

static int hook_low_split_record(struct background_context *bg_context, struct tls_context *context) {
    struct split_record *target_first, *target_last, 
            *record_tail, *record_head = bg_context->split_list.prio_split_list[0];
    uintptr_t *split_target = &(context->prio_split_list.split_list[0]);
    uintptr_t split, record_contex1, record_contex2;
    int idx, record_num;

hook_lock_first_low_split_record :
    split = READ_ONCE(*split_target);

    if (!split)
        return 0;

    idx = first_split_idx(split);
    record_num = split_record_num(split);

    target_first = (struct split_record *)first_split_addr(split);

    record_contex1 = lock_first_low_split_record(target_first, record_head);

    if (unlikely(!record_contex1)) {
        spin_fence();
        goto hook_lock_first_low_split_record;
    }

    WRITE_ONCE(*split_target, 0);

    if (!record_head) {
        bg_context->split_list.prio_split_list[0] = target_first;
        return record_num * SPLIT_ENT_PER_RECORD + idx;
    }

    while (1) {
        target_last = READ_ONCE(target_first->prev_record);
        record_contex2 = locklink_last_low_split_record(target_last, target_first, record_head);

        if (likely(record_contex2))
            break;
    }

    while (1) {
        record_tail = READ_ONCE(record_head->prev_record);
        target_first->prev_record = record_tail;

        if (likely(link_low_split_tail_with_target(record_head, record_tail, target_first)))
            break;
    }

    record_head->prev_record = target_last;

    unlock_split_record(target_last, record_contex2);

    if (target_last != target_first)
        unlock_split_record(target_first, record_contex1);
    
    return record_num * SPLIT_ENT_PER_RECORD + idx;
}

#if BACKGROUNG_THREAD_NUM > 1
static int hook_delay_split_record(struct background_context *bg_context) {
    struct split_record *record_head = bg_context->split_list.prio_split_list[1];
    uintptr_t *split_list = &(bg_context->split_list.delay_split);
    uintptr_t split;
    struct split_record *target_first;
    int idx_num, record_num;

    split = *split_list;

    if (split == 0)
        return 0;

    *split_list = 0;

    target_first = (struct split_record *)first_split_addr(split);
    idx_num = first_split_idx(split);
    record_num = split_record_num(split);

    bg_context->split_list.prio_split_list[1] = target_first;

    if (!record_head)
        bg_context->split_list.prio_split_list[1] = target_first;
    else {
        struct split_record *record_tail = record_head->prev_record, *target_last = target_first->prev_record;
        uintptr_t record_contex;
        int idx;

        idx = split_record_ent_idx(record_tail->next_record);
        record_contex = init_high_locked_split_record(target_first, idx);
        WRITE_ONCE(record_tail->next_record, record_contex);

        idx = split_record_ent_idx(target_last->next_record);
        record_contex = init_high_locked_split_record(record_head, idx);
        WRITE_ONCE(target_last->next_record, record_contex);
        
        record_head->prev_record = target_last;
        target_first->prev_record = record_tail;
    }

    return record_num * SPLIT_ENT_PER_RECORD + idx_num;
}
#endif

__attribute__((always_inline))
static void free_reclaim_chunk(struct reclaim_page *rclist) {
    while (rclist) {
        struct reclaim_page *next = (struct reclaim_page *)next_reclaim_page(rclist->index);
        int i, idx = ent_num_of_reclaim_page(rclist->index);

        for (i = 0; i < idx; ++i)
            free(rclist->entry[i]);

        free_page_aligned(rclist, RECLAIM_PAGE_SIZE);
        rclist = next;
    }
}

__attribute__((always_inline))
static void free_reclaim_page(struct reclaim_page *rclist) {
    while (rclist) {
        struct reclaim_page *next = (struct reclaim_page *)next_reclaim_page(rclist->index);
        int i, idx = ent_num_of_reclaim_page(rclist->index);

        for (i = 0; i < idx; ++i) {
            void *addr = (void *)(((uintptr_t)rclist->entry[i]) & PAGE_MASK); 
            size_t size = 1UL << (((uintptr_t)rclist->entry[i]) & ~PAGE_MASK);
            free_page_aligned(addr, size);
        }

        free_page_aligned(rclist, RECLAIM_PAGE_SIZE);
        rclist = next;
    }
}

__attribute__((always_inline))
static void update_reclaim_contex(struct background_context *bg_context, u64 min_epoch) {
    struct epoched_reclaim_context *chunk_reclaim, *page_reclaim;
    struct reclaim_page *ptail, *ctail;

    chunk_reclaim = &bg_context->chunk_reclaim;
    page_reclaim = &bg_context->page_reclaim;

    if (min_epoch > bg_context->epoch) {
        free_reclaim_page(page_reclaim->wait_rclist);
        free_reclaim_chunk(chunk_reclaim->wait_rclist);
        chunk_reclaim->wait_rclist = page_reclaim->wait_rclist = NULL;
    }

    ctail = chunk_reclaim->rclist_tail;
    ptail = page_reclaim->rclist_tail;

    if (ctail) {
        ctail->index = set_reclaim_page_index(ctail->index, chunk_reclaim->wait_rclist);
        chunk_reclaim->wait_rclist = chunk_reclaim->rclist_head;
        chunk_reclaim->rclist_head = chunk_reclaim->rclist_tail = NULL;
    }

    if (ptail) {
        ptail->index = set_reclaim_page_index(ptail->index, page_reclaim->wait_rclist);
        page_reclaim->wait_rclist = page_reclaim->rclist_head;
        page_reclaim->rclist_head = page_reclaim->rclist_tail = NULL;
    }
}


__attribute__((always_inline))
static u64 scan_tls_context(struct background_context *bg_context) {
    u64 split_num = 0, max_epoch = 0, min_epoch = MAX_LONG_INTEGER;
    int i;

#if BACKGROUNG_THREAD_NUM > 1
    split_num += hook_delay_split_record();
#endif

    for (i = 0; i < THREAD_NUM; ++i) {
        struct tls_context *tls = &tls_context_array[i];
        u64 ep;

        ep = READ_ONCE(tls->epoch);
        acquire_fence();

        if (ep < min_epoch)
            min_epoch = ep;

    #if BACKGROUNG_THREAD_NUM > 1
        if (i < thread_id * PER_THREAD_OF_BACKGROUND || 
		i >= (thread_id + 1) * PER_THREAD_OF_BACKGROUND) {
            if (ep > max_epoch)
                max_epoch = ep;

            continue;
        }
    #endif

        hook_kv_reclaim(bg_context, tls);

        split_num += hook_low_split_record(bg_context, tls);
        split_num += hook_high_split_record(bg_context, tls);

        //don't need memory_fence, implied by prior lock
        ep = READ_ONCE(tls->epoch);
        acquire_fence();

        if (ep > max_epoch)
            max_epoch = ep; 
    }

    update_reclaim_contex(bg_context, min_epoch);

    bg_context->epoch = max_epoch;//*epoch = max_epoch;

    return split_num;
}

__attribute__((always_inline))
static int alloc_eh_two_segment_aligned(int seg2_num, 
                        struct eh_segment **seg_addr, 
                        struct background_context *bg_context) {
    struct free_page_header *p_head;
	void *addr = malloc_prefault_page_aligned((seg2_num + 1) << EH_TWO_SEGMENT_SIZE_BIT);
    size_t n;

	if (unlikely(addr == MAP_FAILED))
		return 0;

	*seg_addr = (struct eh_segment *)(((uintptr_t)addr + EH_TWO_SEGMENT_SIZE - 1) & EH_TWO_SEGMENT_SIZE_MASK);

	if ((void *)(*seg_addr) == addr)
		return seg2_num + 1;

    n = ((uintptr_t)*seg_addr) - (uintptr_t)addr;
	p_head = (struct free_page_header *)addr;
    p_head->next = bg_context->free_page;
    p_head->size = n;

    p_head = (struct free_page_header *)&((*seg_addr)[seg2_num << 1]);
    p_head->next = (struct free_page_header *)addr;
    p_head->size = EH_TWO_SEGMENT_SIZE - n;

    bg_context->free_page = p_head;

	return seg2_num;
}

__attribute__((always_inline))
static void release_free_page(struct background_context *bg_context) {
    struct free_page_header *free_page = bg_context->free_page;

    while (free_page) {
        struct free_page_header *next = free_page->next;
        free_page_aligned(free_page, free_page->size);
        free_page = next;
    }

    bg_context->free_page = NULL;
}

__attribute__((always_inline))
static void prepare_enough_eh_segment(struct background_context *bg_context, int split_num) {
    struct free_page_header *free_seg = bg_context->free_segment;
    struct free_page_header *last = NULL;

    while (free_seg) {
        split_num -= (int)(free_seg->size >> EH_TWO_SEGMENT_SIZE_BIT);
        last = free_seg;
        free_seg = free_seg->next;
    }

    if (split_num > 0) {
        struct eh_segment *seg_addr;
        struct free_page_header *new;
        int ret = alloc_eh_two_segment_aligned(split_num, &seg_addr, bg_context);

        if (unlikely(!ret))
            return;

        new = (struct free_page_header *)seg_addr;
        new->next = NULL;
        new->size = ((size_t)ret) << EH_TWO_SEGMENT_SIZE_BIT;

        if (last)
            last->next = new;
        else
            bg_context->free_segment = new;
    }
}

__attribute__((always_inline))
static int supply_eh_segment(struct background_context *bg_context, 
                                    struct eh_segment **seg_addr) {
    struct free_page_header *free_seg = bg_context->free_segment;
    int num;

    if (free_seg) {
        num = (int)(free_seg->size >> EH_TWO_SEGMENT_SIZE_BIT);
        bg_context->free_segment = free_seg->next;
        *seg_addr = (struct eh_segment *)free_seg;
    } else {
        num = alloc_eh_two_segment_aligned(NUM_OF_EH_SEG_ALLOC, seg_addr, bg_context);

        if (unlikely(!num))
            return 0;

        /*free_seg = (struct free_page_header *)*seg_addr;

        free_seg->next = NULL;
        free_seg->size = ((size_t)num) << EH_TWO_SEGMENT_SIZE_BIT;*/
    }

    return num;
}

__attribute__((always_inline))
static void restore_free_eh_segment(struct background_context *bg_context,
                                    struct eh_segment *seg_addr, int num) {
    struct free_page_header *free_seg = (struct free_page_header *)seg_addr;

    free_seg->next = bg_context->free_segment;
    free_seg->size = ((size_t)num) << EH_TWO_SEGMENT_SIZE_BIT;
    bg_context->free_segment = free_seg;
}

__attribute__((always_inline))
static int process_split_entry(struct background_context *bg_context,
                            struct split_entry *ent, 
                            struct eh_segment **seg_addr, int *seg2_num) {
    uintptr_t bucket_ptr = ent->bucket_ptr;
    struct eh_bucket *bucket;
    int ret, l_depth;

    if (*seg2_num == 0) {
        *seg2_num = supply_eh_segment(bg_context, seg_addr);

        if (unlikely(!(*seg2_num)))
            return -1;
    }

    bucket = (struct eh_bucket *)split_entry_bucket_addr(bucket_ptr);
    l_depth = split_entry_depth(bucket_ptr);

    ret = split_eh_segment(bucket, *seg_addr, ent->hashed_key, l_depth);
            
    if (unlikely(ret == -1))
        return -1;

#if BACKGROUNG_THREAD_NUM > 1
    if (unlikely(ret)) {
        if (unlikely(!append_split_ent_bg(ent->hashed_key, bucket, l_depth)))
            return -1;
        return 0;
    }
#endif

    --(*seg2_num);
    (*seg_addr) += 2;

    return 0;
}

__attribute__((always_inline))
static void handle_incomplete_high_split_list(struct split_record *head,
                                        struct split_record *cur_record,
                                        uintptr_t old_con, int idx) {
    uintptr_t rec_con1, rec_con2;
    struct split_record *tail = head->prev_record;

    cur_record->prev_record = tail;

    rec_con1 = set_split_record_addr(tail->next_record, cur_record);
    WRITE_ONCE(tail->next_record, rec_con1);

    rec_con2 = set_split_record_ent_idx(old_con, idx);
    WRITE_ONCE(cur_record->next_record, rec_con2);
}

__attribute__((always_inline))
static int handle_incomplete_low_split_list(struct split_record *head,
                                        struct split_record *cur_record,
                                        uintptr_t old_con, int idx) {
    uintptr_t rec_con1, rec_con2;
    struct split_record *tail = READ_ONCE(head->prev_record);

    rec_con2 = READ_ONCE(tail->next_record);

    if (cur_record != tail && unlikely(split_record_locked_or_high(rec_con2)))
        return -1;

    if (head != cur_record)
        cur_record->prev_record = tail;

    rec_con1 = set_split_record_addr(rec_con2, cur_record);
    
    if (unlikely(!cas_bool(&tail->next_record, rec_con2, rec_con1)))
        return -1;

    if (idx) {
        if (cur_record == tail)
            old_con = rec_con1;

        rec_con1 = set_split_record_ent_idx(old_con, idx);
        WRITE_ONCE(cur_record->next_record, rec_con1);
    }

    return 0;
}

__attribute__((always_inline))
uintptr_t mark_next_doing_low_record(struct split_record *next) {
    uintptr_t con, rec_con;

    while (1) {
        con = READ_ONCE(next->next_record);

        if (likely(!split_record_locked_or_high(con))) {
            rec_con = set_high_locked_doing_split_record(con);

            if (likely(cas_bool(&next->next_record, con, rec_con)))
                break;
        }

        spin_fence();
    }

    return rec_con;
}

static int process_high_split_list(struct background_context *bg_context) {
    struct split_record *head, *record_head = bg_context->split_list.prio_split_list[1];
    struct eh_segment *seg_addr;
    uintptr_t rec_con;
    int num = 0, idx = 0;

    if (record_head) {
        head = record_head;

        do {
            rec_con = record_head->next_record;
            idx = split_record_ent_idx(rec_con);

            while (idx) {
                if (unlikely(process_split_entry(bg_context, 
                                &record_head->ent[idx - 1], &seg_addr, &num)))
                    goto after_process_high_split;
            
                --idx;
            }

            delay_chunk_to_rcpage(record_head);
            record_head = (struct split_record *)split_record_addr(rec_con);
        } while (record_head != head);
    }

after_process_high_split :

    if (num)
        restore_free_eh_segment(bg_context, seg_addr, num);

    if (unlikely(idx)) {
        handle_incomplete_high_split_list(head, record_head, rec_con, idx);
        bg_context->split_list.prio_split_list[1] = record_head;
        return 1;
    }

    bg_context->split_list.prio_split_list[1] = NULL;

    return 0;
}

static int process_low_split_list(struct background_context *bg_context, u64 time_limit) {
    struct split_record *head, *next, *record_head;
    struct eh_segment *seg_addr;
    uintptr_t rec_con;
    u64 cur_time;
    int i, num = 0, idx = 0;

    record_head = bg_context->split_list.prio_split_list[0];

    if (record_head) {
        head = record_head;
        rec_con = record_head->next_record;

    remain_time_process_low_split_list :
        for (i = 0; i < NUM_OF_EH_SEG_ALLOC; ++i) {
            idx = split_record_ent_idx(rec_con);
            next = (struct split_record *)split_record_addr(rec_con);

            while (idx) {
                if (unlikely(process_split_entry(bg_context, 
                                &record_head->ent[idx - 1], &seg_addr, &num)))
                    goto after_process_low_split;
            
                --idx;
            }

            delay_chunk_to_rcpage(record_head);

            if (next == head)
                goto after_process_low_split;

            rec_con = mark_next_doing_low_record(next);
            record_head = next;
        }

        cur_time = sys_time_us();

        if (cur_time < time_limit)
            goto remain_time_process_low_split_list;
    }

after_process_low_split :

    if (num)
        restore_free_eh_segment(bg_context, seg_addr, num);

    if (record_head != NULL && (next != head || idx)) {
        while (handle_incomplete_low_split_list(head, record_head, rec_con, idx))
            spin_fence();

        bg_context->split_list.prio_split_list[0] = record_head;
        return 1;
    }

    bg_context->split_list.prio_split_list[0] = NULL;

    return 0;
}


void *background_task(void *id) {
    struct background_context *bg_context;
    u64 time_limit;
    int split_num;

    thread_id = (int)id;

    bg_context = &bg_context_array[thread_id];
    tls_context = (struct tls_context *)bg_context;

    while (1) {
        time_limit = sys_time_us() + BACKGROUND_PERIOD;

        split_num = scan_tls_context(bg_context);

        prepare_enough_eh_segment(bg_context, split_num);

        if (process_high_split_list(bg_context))
            release_free_page(bg_context);
        else {
            u64 t;

            if (process_low_split_list(bg_context, time_limit) /*|| to dooooooo*/)
                continue;
            
            t = sys_time_us();

            if (t < time_limit)
                usleep(time_limit - t);
        }
    }

}
