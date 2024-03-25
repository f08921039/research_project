#include "dht_init.h"

#include "kv.h"

#include "fix_hash.h"
#include "ext_hash.h"


extern u64 eh_group[EH_GROUP_NUM];
extern u64 eh_migrate_group[EH_GROUP_NUM];
extern struct fh_bucket fix_htable[FH_BUCKET_NUM];
extern struct tls_context tls_context_array[THREAD_NUM];
extern struct background_context bg_context_array[BACKGROUNG_THREAD_NUM];

extern __thread int thread_id;

struct thread_paramater thread_paramater_array[THREAD_NUM];
pthread_t background_pthread[BACKGROUNG_THREAD_NUM];
int active_thread_num = 0;
int cpu_num = 0;


static void *work_thread_function(void *paramater) {
    struct thread_paramater *t_paramater = (struct thread_paramater *)paramater;

    thread_id = t_paramater->tid;
    tls_context = &tls_context_array[thread_id];
    return t_paramater->callback_fuction(t_paramater->paramater);
}


__attribute__((always_inline))
static void init_fix_htable() {
    memset(&fix_htable[0], 0, FH_BUCKET_NUM * sizeof(struct fh_bucket));
}

static int init_eh_group() {
    struct eh_dir_entry *dir_array;
    struct eh_segment *seg_array;
    void *addr;
    u64 ent1, ent2;
    int all_seg_size, g, i, j;

    all_seg_size = EH_GROUP_NUM * 3
                    * (EH_SEGMENT_SIZE << (INITIAL_EH_L_DEPTH - EH_GROUP_BIT - 1));

    dir_array = (struct eh_dir_entry *)
                    malloc_prefault_page_aligned(PAGE_SIZE * EH_GROUP_NUM);

    if (unlikely((void *)dir_array == MAP_FAILED))
        return -1;

    addr = malloc_prefault_page_aligned(all_seg_size + EH_SEGMENT_SIZE);

    if (unlikely(addr == MAP_FAILED)) {
        free_page_aligned(dir_array, PAGE_SIZE * EH_GROUP_NUM);
        return -1;
    }

    seg_array = (struct eh_segment *)(((uintptr_t)addr + EH_SEGMENT_SIZE - 1) & EH_SEGMENT_SIZE_MASK);

    if ((void *)seg_array == addr) {
        addr += all_seg_size;
        free_page_aligned(addr, EH_SEGMENT_SIZE);
    } else {
        size_t n = (uintptr_t)seg_array - (uintptr_t)addr;
        free_page_aligned(addr, n);

        addr = ((void *)seg_array) + all_seg_size;
        free_page_aligned(addr, EH_SEGMENT_SIZE - n);
    }
	
    memset(seg_array, 0, all_seg_size);

    for (g = 0; g < EH_GROUP_NUM; ++g) {
        eh_group[g] = eh_migrate_group[g] = set_eh_dir_depth(dir_array, INITIAL_EH_G_DEPTH);

        for (i = 0; i < (1 << (INITIAL_EH_L_DEPTH - EH_GROUP_BIT)); ++i) {
            if ((i & 1) == 0)
                ent2 = (uintptr_t)(seg_array++);

            ent1 = (((uintptr_t)(seg_array++)) | INITIAL_EH_L_DEPTH);

            for (j = 0; j < (1 << (INITIAL_EH_G_DEPTH - INITIAL_EH_L_DEPTH)); ++j) {
                dir_array->ent1 = ent1;
                dir_array->ent2 = ent2;
                ++dir_array;
            }
        }

    }

    return 0;
}

static int init_tls_contex() {
    struct tls_context *tls;
    struct reclaim_page *kv_page;
    int i;

    for (i = 0; i < THREAD_NUM; ++i) {
        tls = &tls_context_array[i];
        tls->epoch = 0;
        // to dooooooooooooo tls->count
        tls->kv_rclist_head = tls->kv_rclist_tail = NULL;
        tls->prio_split_list.split_list[0] = tls->prio_split_list.split_list[1] = 0;

        kv_page = (struct reclaim_page *)malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE);

        if (unlikely((void *)kv_page == MAP_FAILED)) {
            tls->kv_rcpage = NULL;

            while (i--) {
                tls = &tls_context_array[i];
                free_page_aligned(tls->kv_rcpage, RECLAIM_PAGE_SIZE);
                tls->kv_rcpage = NULL;
            }
            
            return -1;
        }

        tls->kv_rcpage = kv_page;
    }

    return 0;
}

static void init_background_contex() {
    struct background_context *bg;
    int i;

    for (i = 0; i < BACKGROUNG_THREAD_NUM; ++i) {
        bg = &bg_context_array[i];
        bg->epoch = MAX_LONG_INTEGER;
        //to doooooooooooooo bg->
        bg->free_segment = bg->free_page = NULL;
        bg->chunk_reclaim.rclist_head = bg->chunk_reclaim.rclist_tail
                            = bg->chunk_reclaim.wait_rclist = NULL;
        bg->page_reclaim.rclist_head = bg->page_reclaim.rclist_tail
                            = bg->page_reclaim.wait_rclist = NULL;
        bg->split_list.prio_split_list[0] = bg->split_list.prio_split_list[1] = NULL;

    #if BACKGROUNG_THREAD_NUM > 1
        bg->split_list.delay_split = 0;
    #endif
    }
}



int dht_init_structure() {
    init_fix_htable();

    if (unlikely(init_eh_group()))
        return -1;

    if (unlikely(init_tls_contex())) {
        //to dooooooooooooo free directory and segment memory
        return -1;
    }

    init_background_contex();

    return 0;
}

//to dooooooooooooo assert cpus, threads must be larger than 0
int dht_create_thread(void *(**start_routine)(void *),
                                void **restrict arg, 
                                int threads, int cpus) {
    struct thread_paramater *t_paramater;
    int ret, i, all_cpu, c_num = 0;
    
    if (active_thread_num || threads > THREAD_NUM)
        return -1;

    all_cpu = (threads + BACKGROUNG_THREAD_NUM > cpus);

    active_thread_num = threads;
    cpu_num = cpus;

    for (i = 0; i < BACKGROUNG_THREAD_NUM; ++i) {
        if (all_cpu)
            ret = create_default_thread(&background_pthread[i],
                    		        &background_task, (void *)i);
        else
            ret = create_binding_thread(&background_pthread[i], 
                            &background_task, (void *)i, cpus - 1 - i);

        if (unlikely(ret)) {
            while (i--)
                terminate_the_thread(background_pthread[i]);

            return ret;
        }

    }

    for (i = 0; i < threads; ++i) {
        t_paramater = &thread_paramater_array[i];
        t_paramater->tid = i;
        t_paramater->cpu_id = c_num;
        t_paramater->callback_fuction = start_routine[i];
        t_paramater->paramater = arg[i];

        ret = create_binding_thread(&t_paramater->pthread_id, &work_thread_function, t_paramater, c_num);

        if (unlikely(ret)) {
            while (i--)
                terminate_the_thread(thread_paramater_array[i].pthread_id);

            return ret;
        }

        if (++c_num == cpus)
            c_num = 0;
    }

    return 0;
}

//to dooooo assert threads must be larger than 0
//retrun success-added thread num
int dht_add_thread(void *(**start_routine)(void *),
                    void **restrict arg, int threads) {
    struct thread_paramater *t_paramater;
    int ret, i, c_num, free_cpu;

    if (active_thread_num + threads > THREAD_NUM)
        return 0;    

    free_cpu = cpu_num - (active_thread_num + BACKGROUNG_THREAD_NUM);

    c_num = active_thread_num % cpu_num;

    if (free_cpu < threads)
        for (i = 0; i < BACKGROUNG_THREAD_NUM; ++i)
            unbind_thread(background_pthread[i], cpu_num);
    
    for (i = 0; i < threads; ++i) {
        t_paramater = &thread_paramater_array[active_thread_num];
        t_paramater->tid = active_thread_num;
        t_paramater->cpu_id = c_num;
        t_paramater->callback_fuction = start_routine[i];
        t_paramater->paramater = arg[i];

        ret = create_binding_thread(&t_paramater->pthread_id, &work_thread_function, t_paramater, c_num);

        if (unlikely(ret))
            return i;

        ++active_thread_num;

        if (++c_num == cpu_num)
            c_num = 0;
    }

    return threads;
}
