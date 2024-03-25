#ifndef __PER_THREAD_H
#define __PER_THREAD_H

#include "compiler.h"
#include "kv.h"

#ifdef  __cplusplus
extern  "C" {
#endif

#define MAX_THREAD_NUM  ((1 << 13) - 1)
#define INVALID_THREAD_ID   MAX_THREAD_NUM

#ifndef THREAD_NUM
#define THREAD_NUM  20
#endif


#define RECLAIM_PAGE_SIZE    4096UL
#define RECLAIM_PAGE_ENT_NUM 511UL
#define RECLAIM_PAGE_MASK    (RECLAIM_PAGE_SIZE - 1)

#define set_reclaim_page_index(idx, next_page)  ((idx) | (uintptr_t)(next_page))
#define ent_num_of_reclaim_page(idx)  ((idx) & RECLAIM_PAGE_MASK)
#define next_reclaim_page(idx)  ((idx) & ~RECLAIM_PAGE_MASK)




#define SPLIT_ENT_SIZE  (sizeof(struct split_entry))

#define set_split_entry_bucket_ptr(bucket, depth)   (((uintptr_t)(bucket)) | depth)
#define split_entry_depth(bucket_ptr)   ((bucket_ptr) & 0x00000000000000FF)
#define split_entry_bucket_addr(bucket_ptr) ((bucket_ptr) & ~0x00000000000000FF)



#define SPLIT_RECORD_SIZE   (CACHE_LINE_SIZE << 1)
#define SPLIT_RECORD_SHIFT  (CACHE_LINE_SHIFT + 1)
#define SPLIT_ENT_PER_RECORD    ((SPLIT_RECORD_SIZE - 16) / SPLIT_ENT_SIZE)

#define SPLIT_RECORD_LOCKED_SHIFT   (VALID_POINTER_BITS + 0)
#define SPLIT_RECORD_HIGH_SHIFT   (VALID_POINTER_BITS + 1)
#define SPLIT_RECORD_DOING_SHIFT   (VALID_POINTER_BITS + 2)
#define SPLIT_RECORD_ENT_IDX_BIT    6
#define SPLIT_RECORD_ENT_IDX_MASK   ((1UL << SPLIT_RECORD_ENT_IDX_BIT) - 1)
#define SPLIT_RECORD_TID_BIT    13

#define split_record_locked(record) ((record) & (1UL << SPLIT_RECORD_LOCKED_SHIFT))
#define split_record_high(record) ((record) & (1UL << SPLIT_RECORD_HIGH_SHIFT))
#define split_record_doing(record) ((record) & (1UL << SPLIT_RECORD_DOING_SHIFT))
#define split_record_ent_idx(record)    ((record) & SPLIT_RECORD_ENT_IDX_MASK)
#define split_record_tid(record)    ((record) >> (POINTER_BITS - SPLIT_RECORD_TID_BIT))
#define split_record_addr(record)   ((record) & (VALID_POINTER_MASK & (~SPLIT_RECORD_ENT_IDX_MASK)))

#define split_record_locked_or_high(record)     \
                ((record) & ((1UL << SPLIT_RECORD_LOCKED_SHIFT) | (1UL << SPLIT_RECORD_HIGH_SHIFT)))

#define set_split_record_locked(record) \
                ((record) | (1UL << SPLIT_RECORD_LOCKED_SHIFT))

#define set_split_record_high_locked(record) \
            ((record) | (1UL << SPLIT_RECORD_LOCKED_SHIFT) | (1UL << SPLIT_RECORD_HIGH_SHIFT))

#define inc_split_record_ent_idx(record)    ((record) + 1)
#define reset_split_record_ent_idx(record)    (((record) & ~SPLIT_RECORD_ENT_IDX_MASK) | 1UL)
#define set_split_record_ent_idx(record, idx)    (((record) & ~SPLIT_RECORD_ENT_IDX_MASK) | (idx))
#define set_split_record_addr(record, addr) \
            (((record) & ~(VALID_POINTER_MASK & (~SPLIT_RECORD_ENT_IDX_MASK))) | (uintptr_t)(addr))

#define set_locked_split_record_addr(record, addr)  \
            set_split_record_locked(set_split_record_addr(record, addr))

#define set_high_locked_doing_split_record(record)  \
            ((record) | ((1UL << SPLIT_RECORD_HIGH_SHIFT) |  \
            (1UL << SPLIT_RECORD_LOCKED_SHIFT) | (1UL << SPLIT_RECORD_DOING_SHIFT)))

#define init_low_split_record(next, tid, ent_idx)   \
                (((uintptr_t)(next)) | (ent_idx) |  \
                (((u64)(tid)) << (POINTER_BITS - SPLIT_RECORD_TID_BIT)))

#define init_low_locked_split_record(next, tid, ent_idx)   \
                            (((uintptr_t)(next)) | (ent_idx) |  \
                            (1UL << SPLIT_RECORD_LOCKED_SHIFT) |    \
                            (((u64)(tid)) << (POINTER_BITS - SPLIT_RECORD_TID_BIT)))

#define init_high_split_record(next, tid, ent_idx)  \
                    (((uintptr_t)(next)) | (ent_idx) |  \
                    (1UL << SPLIT_RECORD_HIGH_SHIFT) |  \
            (((u64)INVALID_THREAD_ID) << (POINTER_BITS - SPLIT_RECORD_TID_BIT)))

#define init_high_locked_split_record(next, ent_idx)  \
            (((uintptr_t)(next)) | (ent_idx) |  \
            (1UL << SPLIT_RECORD_HIGH_SHIFT) |  \
            (1UL << SPLIT_RECORD_LOCKED_SHIFT) | \
            (((u64)INVALID_THREAD_ID) << (POINTER_BITS - SPLIT_RECORD_TID_BIT)))

#define init_high_locked_doing_split_record(next, ent_idx)  \
            (((uintptr_t)(next)) | (ent_idx) | (1UL << SPLIT_RECORD_HIGH_SHIFT) |   \
                (1UL << SPLIT_RECORD_LOCKED_SHIFT) |    \
                (1UL << SPLIT_RECORD_DOING_SHIFT) | \
                (((u64)INVALID_THREAD_ID) << (POINTER_BITS - SPLIT_RECORD_TID_BIT)))

/*#define init_high_locked_full_split_record(next)  \
            (((uintptr_t)(next)) | SPLIT_ENT_PER_RECORD |   \
            (1UL << SPLIT_RECORD_HIGH_SHIFT) |  \
            (1UL << SPLIT_RECORD_LOCKED_SHIFT) |   \
            (((u64)INVALID_THREAD_ID) << (POINTER_BITS - SPLIT_RECORD_TID_BIT)))*/





#define FIRST_SPLIT_IDX_MASK  ((1UL << SPLIT_RECORD_SHIFT) - 1)

#define first_split_addr(split)    \
                    ((split) & ((~FIRST_SPLIT_IDX_MASK) & VALID_POINTER_MASK))

#define first_split_idx(split)  ((split) & FIRST_SPLIT_IDX_MASK)
#define split_record_num(split) ((split) >> VALID_POINTER_BITS)
#define set_first_split(record_addr, idx, record_num)  \
                        (((uintptr_t)(record_addr)) | (idx) | (((u64)(record_num)) << VALID_POINTER_BITS))


struct reclaim_page {
    uintptr_t index;
    void *entry[RECLAIM_PAGE_ENT_NUM];
};



struct split_entry {
    u64 hashed_key;
    uintptr_t bucket_ptr; //last 8-bits is l_depth
};


struct split_record {
    uintptr_t next_record;  //63~50th bit is thread_id, 48th bit is lock, 49th bit is high_flag, 0-5th bit is ent_idx
    struct split_record *prev_record;
    struct split_entry ent[SPLIT_ENT_PER_RECORD];
};


struct priority_split_list_tls {
    uintptr_t split_list[2];//struct split_record *ptr + first_record_idx
};

struct tls_context {
    u64 epoch;
    //u32 split_count;
            //
            //
    struct reclaim_page *kv_rcpage;
    struct reclaim_page *kv_rclist_head;
    struct reclaim_page *kv_rclist_tail;
    struct priority_split_list_tls prio_split_list;
}__attribute__((aligned(CACHE_LINE_SIZE)));


extern struct tls_context tls_context_array[THREAD_NUM];

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;

static inline void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

static inline uintptr_t lock_high_split_record(struct split_record *record) {
    uintptr_t next_record = READ_ONCE(record->next_record);

    if (likely(!split_record_locked(next_record))) {
        int ret = cas_bool(&record->next_record, next_record, set_split_record_locked(next_record));

        if (likely(ret))
            return next_record;
    }

    return 0;
}


static inline uintptr_t lock_low_split_record(struct split_record *record) {
    uintptr_t next_record = READ_ONCE(record->next_record);

    if (likely(!split_record_locked_or_high(next_record))) {
        int ret = cas_bool(&record->next_record, next_record, set_split_record_locked(next_record));

        if (likely(ret))
            return next_record;
    }

    return 0;
}

static inline void unlock_split_record(struct split_record *record, u64 unlock_key) {
    release_fence();
    WRITE_ONCE(record->next_record, unlock_key);
}


void *append_low_split_ent(uintptr_t *split_addr, u64 hashed_key, void *bucket, int l_depth);
void *append_high_split_ent(uintptr_t *split_addr, u64 hashed_key, void *bucket, int l_depth);

static inline void *append_low_split_ent_tls(u64 hashed_key, void *bucket, int l_depth) {
    uintptr_t *split_addr = &(tls_context->prio_split_list.split_list[0]);
    return append_low_split_ent(split_addr, hashed_key, bucket, l_depth);
}


static inline void *append_high_split_ent_tls(u64 hashed_key, void *bucket, int l_depth) {
    uintptr_t *split_addr = &(tls_context->prio_split_list.split_list[1]);
    return append_high_split_ent(split_addr, hashed_key, bucket, l_depth);
}


void upgrade_split_record(struct split_record *record);

int reclaim_kv_to_rcpage(struct kv *kv);


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
