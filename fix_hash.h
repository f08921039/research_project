#ifndef __FIX_HASH_H
#define __FIX_HASH_H

#include "compiler.h"
#include "per_thread.h"
#include "kv.h"

#include "ext_hash.h"

#define FH_BUCKET_NUM_BIT	21

#if FH_BUCKET_NUM_BIT < EH_BUCKET_INDEX_BIT
	#define FH_BUCKET_NUM_BIT	EH_BUCKET_INDEX_BIT
#endif

#define FH_BUCKET_NUM	(1UL << FH_BUCKET_NUM_BIT)
#define FH_BUCKET_SLOT	15
#define FH_BUCKET_SIZE	((FH_BUCKET_SLOT + 1) << 3)
#define FH_BUCKET_CACHELINE	(FH_BUCKET_SIZE >> CACHE_LINE_SHIFT)


#ifdef  __cplusplus
extern  "C" {
#endif

#define INVALID_FH_BUCKET_HEADER	0xFFFFFFFFFFFFFFFF

#define FH_BUCKET_HEADER_FLAG_SHIFT	3
#define FH_BUCKET_HEADER_FLAG_MASK	((1UL << FH_BUCKET_HEADER_FLAG_SHIFT) - 1)
#define FH_BUCKET_HEADER_LOCKED_BIT	0
#define FH_BUCKET_HEADER_BATCH_BIT	1
#define FH_BUCKET_HEADER_TAIL_IDX_BIT	VALID_POINTER_BITS
#define FH_BUCKET_HEADER_TAIL_IDX_MASK	VALID_POINTER_MASK


#define next_fh_bucket(header)	((header) & (FH_BUCKET_HEADER_TAIL_IDX_MASK ^ FH_BUCKET_HEADER_FLAG_MASK))
#define fh_bucket_locked(header)	((header) & (1UL << FH_BUCKET_HEADER_LOCKED_BIT))
#define fh_bucket_batching(header)	(!((header) & (1UL << FH_BUCKET_HEADER_BATCH_BIT)))
#define fh_bucket_tail_idx(header)	((header) >> FH_BUCKET_HEADER_TAIL_IDX_BIT)

#define set_fh_bucket_locked(header)	((header) | (1UL << FH_BUCKET_HEADER_LOCKED_BIT))
#define clear_fh_bucket_batching(header)	((header) | (1UL << FH_BUCKET_HEADER_BATCH_BIT))
#define set_fh_bucket_tail_idx(header, idx)	\
				(((header) & FH_BUCKET_HEADER_TAIL_IDX_MASK) |	\
				((u64)(idx)) << FH_BUCKET_HEADER_TAIL_IDX_BIT)

#define set_fh_bucket_next_bucket(header, next_bucket)	\
						(((header) & ~(FH_BUCKET_HEADER_TAIL_IDX_MASK ^ FH_BUCKET_HEADER_FLAG_MASK))	\
																		| (uintptr_t)(next_bucket))
#define make_fh_link_bucket_header(next_bucket)	((uintptr_t)(next_bucket))



#define FH_ENTRY_FLAG_SHIFT	3
#define FH_ENTRY_FLAG_MASK	((1UL << FH_ENTRY_FLAG_SHIFT) - 1)
#define FH_ENTRY_KEPT_FLAG_BIT	0

#define fh_entry_fingerprint16(entry)	((entry) >> VALID_POINTER_BITS)
#define fh_entry_kv_addr(entry)	((entry) & (VALID_POINTER_MASK ^ FH_ENTRY_FLAG_MASK))
#define fh_entry_valid(entry)	(entry)
#define fh_entry_kept(entry)	((entry) & (1UL << FH_ENTRY_KEPT_FLAG_BIT))

#define set_fh_entry_kept(entry)	((entry) | (1UL << FH_ENTRY_KEPT_FLAG_BIT))
#define clean_fh_entry_kept(entry)	((entry) & ~(1UL << FH_ENTRY_KEPT_FLAG_BIT))

#define make_fh_new_entry(kv, fingerprint16)	set_fh_entry_kept(((uintptr_t)(kv)) | ((fingerprint16) << VALID_POINTER_BITS))

#define FH_ENTRY_WRITEBACK_THREHOLD	4



#define fh_index(hashed_key)	((hashed_key) >> (64 - FH_BUCKET_NUM_BIT))
#define fh_fingerprint16(hashed_key)	((hashed_key) & ((1UL << 16) - 1))
#define fh_bucket_kept_map_mask	((1 << FH_BUCKET_SLOT) - 1)



struct fh_bucket {
	u64 header;
	u64 ent[FH_BUCKET_SLOT];
}__attribute__ ((packed));



extern u64 eh_group[EH_GROUP_NUM];

extern struct fh_bucket fix_htable[FH_BUCKET_NUM];


static inline struct fh_bucket *get_fh_bucket(u64 hashed_key) {
	return &fix_htable[fh_index(hashed_key)];
}


static inline u64 lock_fh_bucket(struct fh_bucket *bucket) {
	u64 old, header = READ_ONCE(bucket->header);

	while (1) {
		if (unlikely(fh_bucket_locked(header))) {//printf("hiiiiiiiiiiiiiiii\n");
			spin_fence();
			header = READ_ONCE(bucket->header);
			continue;
		}

		old = cas(&bucket->header, header, set_fh_bucket_locked(header));

		if (likely(old == header))
			return old;

		header = old;//printf("hoooooooooooooooooooooooo\n");
	}
}

static inline void unlock_fh_bucket(struct fh_bucket *bucket, u64 old_header) {
	release_fence();
	WRITE_ONCE(bucket->header, old_header);
}

__attribute__((optimize("unroll-loops")))
static inline void prefetch_fh_bucket(struct fh_bucket *bucket) {
	int i;
	for (i = 0; i < FH_BUCKET_CACHELINE; ++i) {
		void *addr = (void *)(((uintptr_t)bucket) + (i << CACHE_LINE_SHIFT));
		prefech_r0(addr);
	}
}


/*static inline struct fh_bucket *locate_fh_bucket(u64 hashed_key,
								u64 *fingerprint16, int *group) {
	struct fh_bucket *bucket = get_fh_bucket(hashed_key);

	prefetch_fh_bucket(bucket);

	prefech_r0(&eh_group[0]);

	return bucket;
}*/

int put_kv_fh_bucket(struct kv *kv, u64 hashed_key, 
				struct fh_bucket *bucket,
				u64 fingerprint16, int group);


int get_kv_fh_bucket(struct kv *kv, u64 hashed_key,
				struct fh_bucket *bucket,
				u64 fingerprint16, int group);

static inline int put_kv(struct kv *kv, u64 hashed_key) {
	struct fh_bucket *bucket;
	u64 fingerprint16;
	int group;

	bucket = get_fh_bucket(hashed_key);

	prefetch_fh_bucket(bucket);

	fingerprint16 = fh_fingerprint16(hashed_key);
	group = eh_group_index(hashed_key);

	prefech_r0(&eh_group[0]);

	return put_kv_fh_bucket(kv, hashed_key, bucket, fingerprint16, group);
}


static inline int get_kv(struct kv *kv, u64 hashed_key) {
	struct fh_bucket *bucket;
	u64 fingerprint16;
	int group;

	bucket = get_fh_bucket(hashed_key);

	prefetch_fh_bucket(bucket);

	fingerprint16 = fh_fingerprint16(hashed_key);
	group = eh_group_index(hashed_key);

	prefech_r0(&eh_group[0]);

	return get_kv_fh_bucket(kv, hashed_key, bucket, fingerprint16, group);
}


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__FIX_HASH_H
