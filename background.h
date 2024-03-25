#ifndef __BACKGROUND_H
#define __BACKGROUND_H

#define BACKGROUNG_THREAD_NUM   1

#include "compiler.h"
#include "per_thread.h"


#define PER_THREAD_OF_BACKGROUND    (THREAD_NUM / BACKGROUNG_THREAD_NUM)

#define BACKGROUND_PERIOD   2000

#ifdef  __cplusplus
extern  "C" {
#endif

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;
extern struct tls_context tls_context_array[THREAD_NUM];

struct free_page_header {
    struct free_page_header *next;
    size_t size;
};

struct epoched_reclaim_context {
    struct reclaim_page *rclist_head;
    struct reclaim_page *rclist_tail;
    struct reclaim_page *wait_rclist;
    //struct reclaim_page *free_rclist;
};

struct priority_split_list {
    struct split_record *prio_split_list[2];
#if BACKGROUNG_THREAD_NUM > 1
    uintptr_t delay_split;
#endif
};


struct background_context {
    u64 epoch;
    //
    struct free_page_header *free_segment;
    struct free_page_header *free_page;
    struct epoched_reclaim_context chunk_reclaim;
    struct epoched_reclaim_context page_reclaim;
    struct priority_split_list split_list;
}__attribute__((aligned(CACHE_LINE_SIZE)));


struct background_context bg_context_array[BACKGROUNG_THREAD_NUM];


int delay_to_rcpage(struct epoched_reclaim_context *reclaim_ctx, 
                                                    void *reclaim_ent);


static inline struct background_context *cast_ptr_to_bg_ctx(void *ptr) {
    return (struct background_context *)ptr;
}

#if BACKGROUNG_THREAD_NUM > 1
static inline int append_split_ent_bg(u64 hashed_key, void *bucket, int l_depth) {
    struct background_context *bg_context = cast_ptr_to_bg_ctx(tls_context);
    uintptr_t *split_addr = &(bg_context->split_list.delay_split);
    return append_high_split_ent(hashed_key, bucket, split_addr, l_depth);
}
#endif


static inline int delay_chunk_to_rcpage(void *reclaim_ent) {
    struct background_context *bg_context = cast_ptr_to_bg_ctx(tls_context);
    struct epoched_reclaim_context *reclaim_ctx = &bg_context->chunk_reclaim;
    return delay_to_rcpage(reclaim_ctx, reclaim_ent);
}

static inline int delay_page_to_rcpage(void *reclaim_ent, size_t size_shift) {
    struct background_context *bg_context = cast_ptr_to_bg_ctx(tls_context);
    struct epoched_reclaim_context *reclaim_ctx = &bg_context->page_reclaim;
    uintptr_t ent = ((uintptr_t)reclaim_ent) | size_shift;
    return delay_to_rcpage(reclaim_ctx, (void *)ent);
}


void *background_task(void *id);

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__BACKGROUND_H
