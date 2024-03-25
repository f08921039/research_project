#include "ext_hash.h"
#include "per_thread.h"

#include "background.h"
#include "fix_hash.h"

u64 eh_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));
u64 eh_migrate_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));

#define MATCHED_FINGERPRINT_SIMD4_MASK1	0x000000000000FFFF
#define MATCHED_FINGERPRINT_SIMD4_MASK2	0x00000000FFFF0000
#define MATCHED_FINGERPRINT_SIMD4_MASK3	0x0000FFFF00000000
#define MATCHED_FINGERPRINT_SIMD4_MASK4	0xFFFF000000000000

#define fingerprint_simd4_matched(fingerprint, id)	\
			((!((fingerprint) & MATCHED_FINGERPRINT_SIMD4_MASK##id)) << ((id) - 1))

#define EH_BUCKET_CACHELINE_PREFETCH_STEP	4



__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_bucket_head(void *bucket) {
	int i;

	for (i = 0; i < EH_BUCKET_CACHELINE_PREFETCH_STEP; ++i) {
		prefech_r0(bucket);
		bucket = bucket + CACHE_LINE_SIZE;
	}
}

__attribute__((always_inline))
static void prefetch_eh_bucket_step(void *bucket1_end, void *bucket2_begin, void *current_addr) {
	current_addr = current_addr + (EH_BUCKET_CACHELINE_PREFETCH_STEP << CACHE_LINE_SHIFT);
	
	if (current_addr < bucket1_end)
		prefech_r0(current_addr);
	else if (bucket2_begin)
		prefech_r0(bucket2_begin + (current_addr - bucket1_end));
}


__attribute__((always_inline))
static void do_upgrade_split_record(struct eh_dir_entry *dir_ent,
									struct eh_dir_entry *ent) {
	uintptr_t split_entry;
	u64 ent1;

	if (!eh_seg_priority(ent->ent1)) {
		ent1 = set_high_prio_eh_seg(ent->ent1);
		
		if (unlikely(!cas_bool(&dir_ent->ent1, ent->ent1, ent1)))
			return;

		split_entry = ((ent->ent1 >> 48) << 32) |
						 ((ent->ent2 >> 48) << 16) | 
						 ((ent->ent2 & ((1UL << 12) - 1)) << 4);

		upgrade_split_record((struct split_record *)split_entry);
	}
}


__attribute__((always_inline))
static void finish_eh_seg_marking(struct eh_dir_entry *dir_ent, 
								struct eh_dir_entry *ent,
								uintptr_t split_entry, int high_prio) {
	if (high_prio)
		ent->ent1 = set_high_prio_eh_seg_spliting(ent->ent1);
	else {
		ent->ent2 |= ((split_entry >> 4) & ((1UL << 12) - 1)) | 
								((split_entry >> 16) << 48);
		ent->ent1 |= ((split_entry >> 32) << 48);
		ent->ent1 = set_eh_seg_spliting(ent->ent1);

		dir_ent->ent2 = ent->ent2;
	}

	release_fence();
	WRITE_ONCE(dir_ent->ent1, ent->ent1);
}

static uintptr_t mark_eh_seg_spliting(struct eh_dir_entry *dir_ent,
								struct eh_dir_entry *ent, 
								void *half_seg, u64 hashed_key, 
								int g_depth, int l_depth) {
	struct eh_dir_entry tmp_ent;
	uintptr_t split_ent = 0;
	struct eh_dir_entry *dent;

	if (!eh_seg_spliting(ent->ent1)) {
	re_mark_eh_seg_spliting :
		if (unlikely(eh_seg_migrate(ent->ent1))) {
			int group = eh_group_index(hashed_key);
			u64 group_ent = READ_ONCE(eh_migrate_group[group]);
			int depth = eh_dir_depth(group_ent);

			if (unlikely(g_depth + EH_EXTENSION_STEP != depth)) {
				group_ent = READ_ONCE(eh_group[group]);
				depth = eh_dir_depth(group_ent);
			}

			dent = (struct eh_dir_entry *)extract_eh_dir_addr(group_ent);
			dent += eh_dir_floor_index(hashed_key, l_depth, depth);

			tmp_ent.ent1 = READ_ONCE(dent->ent1);
			acquire_fence();
			tmp_ent.ent2 = ent->ent2;

			if (eh_seg_changed(ent->ent1, tmp_ent.ent1))
				return 0;

			split_ent = mark_eh_seg_spliting(dent, &tmp_ent, half_seg, hashed_key, depth, l_depth);

			if (!split_ent)
				return 0;
		}

		tmp_ent.ent1 = set_eh_seg_locked_spliting(ent->ent1);

		if (likely(cas_bool(&dir_ent->ent1, ent->ent1, tmp_ent.ent1))) {
			if (!split_ent) {
				//to dooooooooooooooooo append_low_split_ent_tls
				split_ent = (uintptr_t)append_high_split_ent_tls(hashed_key, half_seg, l_depth);
	
				if (unlikely(!split_ent)) {
					WRITE_ONCE(dir_ent->ent1, ent->ent1);
					return 0;
				}
			}

			finish_eh_seg_marking(dir_ent, ent, split_ent, 1);
			return split_ent;
		}

		tmp_ent.ent1 = READ_ONCE(dir_ent->ent1);

		if (eh_seg_changed(ent->ent1, tmp_ent.ent1))
			return 0;

		ent->ent1 = tmp_ent.ent1;
		goto re_mark_eh_seg_spliting;
	}

	//to dooooooooooooooooooo do_upgrade_split_record(dir_ent, ent)

	return 0;
}


__attribute__((always_inline))
static int get_two_eh_seg_ptr(struct eh_dir_entry *dir_ent,
								struct eh_dir_entry *ent,
								struct eh_segment **high_seg, 
								struct eh_segment **low_seg) {
	while (1) {
		ent->ent1 = READ_ONCE(dir_ent->ent1);
		acquire_fence();

		ent->ent2 = READ_ONCE(dir_ent->ent2);
		acquire_fence();

		if (likely(!eh_seg_locked(ent->ent1)) && 
				likely(ent->ent1 == READ_ONCE(dir_ent->ent1))) {
			*high_seg = (struct eh_segment *)extract_eh_seg_addr(ent->ent1);
			*low_seg = (struct eh_segment *)extract_eh_seg_addr(ent->ent2);
			return eh_seg_depth(ent->ent1);
		}
	}
}

static int eh_update_entry_batch4(struct eh_bucket *bucket,
							struct eh_bucket *upper_bucket, 
							u64 fingerprint_simd4,
							EH_KV_ITEM *kv_array, int *finish_map) {
	struct kv *kv1, *kv2;
	u64 fingerprint, entry, old_entry;
	void *bucket_end;
	int i, j, map, unfinish_map = ~(*finish_map);

	bucket_end = ((void *)bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(bucket->kv[i]);
		acquire_fence();

		if (unlikely(eh_entry_end(entry))) {
			i = EH_PER_BUCKET_KV_NUM + 1;
			goto leave_eh_update_entry_batch4;
		}

		if (!eh_entry_valid(entry)) {
		leave_eh_update_entry_batch4 :
			if (upper_bucket)
				prefetch_eh_bucket_head(upper_bucket);

			return i;
		}

		if ((i & (PER_CACHELINE_EH_KV_ITEM - 1)) == 0)
			prefetch_eh_bucket_step(bucket_end, upper_bucket, &bucket->kv[i]);

		fingerprint = eh_entry_fingerprint16(entry);
		fingerprint = fingerprint | (fingerprint << 16);
		fingerprint = fingerprint | (fingerprint << 32);
		fingerprint ^= fingerprint_simd4;

		map = fingerprint_simd4_matched(fingerprint, 1) |
				fingerprint_simd4_matched(fingerprint, 2) |
				fingerprint_simd4_matched(fingerprint, 3) |
				fingerprint_simd4_matched(fingerprint, 4);
		map &= unfinish_map;
	
		while (map) {
			j = lsb32(map);
			kv1 = (struct kv *)eh_entry_kv_addr(entry);
			kv2 = (struct kv *)eh_entry_kv_addr(kv_array[j]);

			if (compare_kv_key(kv1, kv2))
				map &= ~(1 << j);
			else {
				if (eh_entry_split(entry))
					unfinish_map &= ~(0x00000001 << j);
				else {
					old_entry = cas(&bucket->kv[i], entry, kv_array[j]);

					if (unlikely(old_entry != entry)) {
						entry = old_entry;
						continue;
					}

					unfinish_map &= ~(0x00000101 << j);
					reclaim_kv_to_rcpage(kv1);
				}

				*finish_map = ~unfinish_map;

				if (!(unfinish_map & 0x0000000F))
					return -1;

				break;
			}
		}
	}

	return EH_PER_BUCKET_KV_NUM;
}


static int eh_append_entry_batch4(struct eh_bucket *bucket, int index,
									EH_KV_ITEM *kv_array, int *finish_map) {
	u64 old;
	int i, j, unfinish_map = ~(*finish_map);

	for (i = index; i < EH_PER_BUCKET_KV_NUM; ++i) {
		j = lsb32(unfinish_map);
		old = cas(&bucket->kv[i], 0, kv_array[j]);

		if (unlikely(old)) {
			if (eh_entry_end(old)) {
				i = EH_PER_BUCKET_KV_NUM + 1;
				break;
			}
			
			continue;
		}

		unfinish_map &= ~(0x00000101 << j);

		if (!(unfinish_map & 0x0000000F))
			break;
	}

	*finish_map = ~unfinish_map;
	return i;
}

int writeback_eh_entry_batch4(struct eh_dir_entry *dir_ent, 
						u64 hashed_key, u64 fingerprint_simd4, 
						int g_depth, int ent_num, EH_KV_ITEM *kv_array) {
	struct eh_dir_entry ent;
	struct eh_segment *high_seg, *low_seg;
	struct eh_bucket *high_bucket, *low_bucket;
	int low_index, high_index, l_depth, finish_map;

	l_depth = get_two_eh_seg_ptr(dir_ent, &ent, &high_seg, &low_seg);

	low_bucket = &low_seg->bucket[eh_bucket_index(hashed_key, l_depth - 1)];
	prefetch_eh_bucket_head(low_bucket);

	finish_map = 0x0000000F ^ ((1 << ent_num) - 1);
	finish_map = (finish_map << 8) | finish_map;

	high_bucket = &high_seg->bucket[eh_bucket_index(hashed_key, l_depth)];

	dir_ent -= (eh_dir_index(hashed_key, g_depth) 
					- eh_dir_floor_index(hashed_key, l_depth, g_depth));

	low_index = eh_update_entry_batch4(low_bucket, high_bucket, 
							fingerprint_simd4, kv_array, &finish_map);

	if (unlikely(low_index == -1))
		return finish_map >> 8;

	high_index = eh_update_entry_batch4(high_bucket, NULL, 
							fingerprint_simd4, kv_array, &finish_map);

	if (unlikely(high_index == -1 
			|| high_index == EH_PER_BUCKET_KV_NUM + 1))
		return finish_map >> 8;

	low_index = eh_append_entry_batch4(low_bucket, low_index, 
											kv_array, &finish_map);

	if (low_index < EH_PER_BUCKET_KV_NUM)
		return finish_map >> 8;

	high_index = eh_append_entry_batch4(high_bucket, high_index, 
											kv_array, &finish_map);
		
	if (high_index == EH_PER_BUCKET_KV_NUM 
					&& low_index == EH_PER_BUCKET_KV_NUM) {
		void *half_seg = (void *)round_down_to_half_seg(low_bucket);
		mark_eh_seg_spliting(dir_ent, &ent, half_seg, hashed_key, g_depth, l_depth);
	}

	return finish_map >> 8;
}

/*int writeback_eh_entry_single(struct eh_dir_entry *dir_ent, 
							u64 hashed_key, int g_depth, 
							EH_KV_ITEM kv_entry) {
	struct eh_dir_entry ent;
	struct eh_segment *high_seg, *low_seg;
	struct eh_bucket *high_bucket, *low_bucket;
	struct kv *kv = (struct kv *)eh_entry_kv_addr(kv_entry);
	void *bucket_end;
	u64 entry;
	int i, low_index, high_index, l_depth;

	l_depth = get_two_eh_seg_ptr(dir_ent, &ent, &high_seg, &low_seg);

	low_bucket = &low_seg->bucket[eh_bucket_index(hashed_key, l_depth - 1)];	
	prefetch_eh_bucket_head(low_bucket);

	high_bucket = &high_seg->bucket[eh_bucket_index(hashed_key, l_depth)];
	dir_ent -= (eh_dir_index(hashed_key, g_depth) - eh_dir_floor_index(hashed_key, l_depth, g_depth));

	low_index = high_index = EH_PER_BUCKET_KV_NUM;

	bucket_end = ((void *)low_bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(low_bucket->kv[i]);
		acquire_fence();

		if (unlikely(eh_entry_end(entry))) {
			low_index = EH_PER_BUCKET_KV_NUM + 1;
			prefetch_eh_bucket_head(high_bucket);
			break;
		}
		
		if (!eh_entry_valid(entry)) {
			low_index = i;
			break;
		}

		if (!(i & (PER_CACHELINE_EH_KV_ITEM - 1)))
			prefetch_eh_bucket_step(bucket_end, high_bucket, &low_bucket->kv[i]);

		if (compare_eh_entry_fingerprint16(entry, kv_entry)) {
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv, tmp_kv) == 0) {
				if (!eh_entry_split(entry) && 
							cas_bool(&low_bucket->kv[i], entry, kv_entry)) {
					reclaim_kv_to_rcpage(tmp_kv);
					return 1;			
				}
				return 0;
			}
		}
	}

	bucket_end = ((void *)high_bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(high_bucket->kv[i]);
		acquire_fence();

		if (unlikely(eh_entry_end(entry)))
			return 0;
		
		if (!eh_entry_valid(entry)) {
			high_index = i;
			break;
		}

		if (!(i & (PER_CACHELINE_EH_KV_ITEM - 1)))
			prefetch_eh_bucket_step(bucket_end, NULL, &high_bucket->kv[i]);

		if (compare_eh_entry_fingerprint16(entry, kv_entry)) {
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv, tmp_kv) == 0) {
				if (!eh_entry_split(entry) && 
							cas_bool(&high_bucket->kv[i], entry, kv_entry)) {
					reclaim_kv_to_rcpage(tmp_kv);
					return 1;
				}
				return 0;
			}
		}
	}

	for (i = low_index; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = cas(&low_bucket->kv[i], 0, kv_entry);

		if (unlikely(entry)) {
			if (eh_entry_end(entry)) {
				low_index = EH_PER_BUCKET_KV_NUM + 1;
				break;
			}
			continue;
		}
		return 1;
	}

	for (i = high_index; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = cas(&high_bucket->kv[i], 0, kv_entry);

		if (unlikely(entry)) {
			if (eh_entry_end(entry))
				return 0;
			continue;
		}
		return 1;
	}

	if (low_index != EH_PER_BUCKET_KV_NUM + 1) {
		mark_eh_seg_spliting(dir_ent, &ent, round_down_to_half_seg(low_bucket), hashed_key, l_depth);
	}

	return 0;
}*/

u64 lookup_eh_entry(struct eh_dir_entry *dir_ent, 
					struct kv *kv, u64 hashed_key, 
					u64 fingerprint, int g_depth) {
	struct eh_dir_entry ent;
	struct eh_segment *high_seg, *low_seg;
	struct eh_bucket *high_bucket, *low_bucket;
	void *bucket_end;
	u64 entry;
	int i, l_depth;

	l_depth = get_two_eh_seg_ptr(dir_ent, &ent, &high_seg, &low_seg);

	low_bucket = &low_seg->bucket[eh_bucket_index(hashed_key, l_depth - 1)];	
	prefetch_eh_bucket_head(low_bucket);

	high_bucket = &high_seg->bucket[eh_bucket_index(hashed_key, l_depth)];
	bucket_end = ((void *)low_bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(low_bucket->kv[i]);
		acquire_fence();

		if (unlikely(eh_entry_end(entry)) || !eh_entry_valid(entry)) {
			prefetch_eh_bucket_head(high_bucket);
			break;
		}

		if (!(i & (PER_CACHELINE_EH_KV_ITEM - 1)))
			prefetch_eh_bucket_step(bucket_end, high_bucket, &low_bucket->kv[i]);

		if (eh_entry_fingerprint16(entry) == fingerprint) {
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv, tmp_kv) == 0) {
				if (unlikely(deleted_kv(tmp_kv)))
					return 0;
				copy_kv_val(kv, tmp_kv);
				return clean_fh_entry_kept(entry);
			}
		}
	}

	bucket_end = ((void *)high_bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(high_bucket->kv[i]);
		acquire_fence();

		if (unlikely(eh_entry_end(entry)) || !eh_entry_valid(entry))
			break;

		if (!(i & (PER_CACHELINE_EH_KV_ITEM - 1)))
			prefetch_eh_bucket_step(bucket_end, NULL, &high_bucket->kv[i]);

		if (eh_entry_fingerprint16(entry) == fingerprint) {
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv, tmp_kv) == 0) {
				if (unlikely(deleted_kv(tmp_kv)))
					return 0;
				copy_kv_val(kv, tmp_kv);
				return clean_fh_entry_kept(entry);
			}
		}
	}

	return 0;
}




__attribute__((always_inline))
static int get_two_eh_dir_entry(struct eh_dir_entry *dir_ent,
									struct eh_dir_entry *ent) {
	while (1) {
		ent->ent1 = READ_ONCE(dir_ent->ent1);
		acquire_fence();

		ent->ent2 = READ_ONCE(dir_ent->ent2);
		acquire_fence();

		if (likely(!eh_seg_locked(ent->ent1)) && 
					likely(ent->ent1 == READ_ONCE(dir_ent->ent1)))
			return eh_seg_depth(ent->ent1);
	}
}

static void update_eh_dir_entry(struct eh_dir_entry *dir_ent, 
								struct eh_segment *new_two_seg, 
								u64 hashed_key, int group, 
								int l_depth, int g_depth) {
	struct eh_dir_entry ent;
	struct eh_dir_entry *tmp_ent, *ent_half, *ent_end;
	u64 num, ent1;

	num = 1UL << (g_depth - l_depth);

	ent_half = dir_ent + (num >> 1);
	ent_end = dir_ent + num;

	ent.ent1 = READ_ONCE(dir_ent->ent1);
	acquire_fence();

lock_for_update_eh_dir_entry :
	if (unlikely(eh_seg_migrate(ent.ent1))) {
		u64 group_ent = READ_ONCE(eh_migrate_group[group]);
		int depth = eh_dir_depth(group_ent);

		if (unlikely(g_depth + EH_EXTENSION_STEP != depth)) {
			group_ent = READ_ONCE(eh_group[group]);
			depth = eh_dir_depth(group_ent);
		}

		tmp_ent = (struct eh_dir_entry *)extract_eh_dir_addr(group_ent);
		tmp_ent += eh_dir_floor_index(hashed_key, l_depth, depth);
		
		update_eh_dir_entry(tmp_ent, new_two_seg, hashed_key, group, l_depth, depth);
	}

	//ent1 = cas(&dir_ent->ent1, ent.ent1, set_eh_seg_locked(ent.ent1));
	ent1 = cas(&dir_ent->ent1, ent.ent1, set_eh_seg_locked(0));

	if (unlikely(ent1 != ent.ent1)) {
		ent.ent1 = ent1;
		goto lock_for_update_eh_dir_entry;
	}

	ent.ent2 = extract_eh_seg_addr(ent1);
	ent.ent1 = ((uintptr_t)&new_two_seg[1]) | (l_depth + 1);

	WRITE_ONCE(ent_half->ent1, set_eh_seg_locked(0));
	release_fence();
	WRITE_ONCE(ent_half->ent2, ent.ent2);

	for (tmp_ent = ent_half + 1; tmp_ent < ent_end; ++tmp_ent) {
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, set_eh_seg_locked(0));
		release_fence();
		WRITE_ONCE(tmp_ent->ent2, ent.ent2);
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, ent.ent1);
	}

	release_fence();
	WRITE_ONCE(ent_half->ent1, ent.ent1);

	ent.ent1 = ((uintptr_t)&new_two_seg[0]) | (l_depth + 1);

	for (tmp_ent = dir_ent + 1; tmp_ent < ent_half; ++tmp_ent) {
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, set_eh_seg_locked(0));
		release_fence();
		WRITE_ONCE(tmp_ent->ent2, ent.ent2);
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, ent.ent1);
	}

	release_fence();
	WRITE_ONCE(dir_ent->ent2, ent.ent2);

	release_fence();
	WRITE_ONCE(dir_ent->ent1, ent.ent1);
}

#if BACKGROUNG_THREAD_NUM > 1
static void unable_upgrade_split_record(struct eh_dir_entry *dir, 
									u64 hash_key, int group,
									int g_depth, int l_depth) {
	struct eh_dir_entry *dir_ent = dir + eh_dir_floor_index(hashed_key, l_depth, g_depth);
	u64 old, ent1 = READ_ONCE(dir_ent->ent1);

re_unable_upgrade_split_record :
	if (!eh_seg_priority(ent1)) {
		if (eh_seg_migrate(ent1)) {
			u64 group_ent = READ_ONCE(eh_migrate_group[group]);
			int depth = eh_dir_depth(group_ent);

			if (unlikely(g_depth + EH_EXTENSION_STEP != depth)) {
				group_ent = READ_ONCE(eh_group[group]);
				depth = eh_dir_depth(group_ent);
			}

			dir = (struct eh_dir_entry *)extract_eh_dir_addr(group_ent);
			unable_upgrade_split_record(dir, hash_key, group, depth, l_depth);
		}

		old = cas(&dir_ent->ent1, ent1, set_high_prio_eh_seg(ent1));

		if (unlikely(old != ent1)) {
			ent1 = old;
			goto re_unable_upgrade_split_record;
		}
	}
}
#endif

static int extend_eh_directory(struct eh_dir_entry *old_dir,
								u64 old_dir_val,
								int g_depth, int group) {
	u64 t_ent, total_num, num, new_dir_val;
	struct eh_dir_entry ent;
	struct eh_dir_entry *new_dir, *old_dir_end;
	int l_depth, i;

	old_dir_end = old_dir + (1UL << (g_depth - EH_GROUP_BIT));

	g_depth += EH_EXTENSION_STEP;

	total_num = sizeof(struct eh_dir_entry) << (g_depth - EH_GROUP_BIT);
	new_dir = (struct eh_dir_entry *)malloc_prefault_page_aligned(total_num);

	if (unlikely((void *)new_dir == MAP_FAILED))
		return -1;

	new_dir_val = set_eh_dir_depth(new_dir, g_depth);

#if BACKGROUNG_THREAD_NUM > 1
	if (unlikely(!cas_bool(&eh_migrate_group[group], old_dir_val, new_dir_val))) {
		if (free_page_aligned(new_dir, total_num))
			return -1;

		unable_upgrade_split_record(old_dir, hash_key, group,
									g_depth - EH_EXTENSION_STEP, l_depth);
		return 1;
	}
#else
	release_fence();
	WRITE_ONCE(eh_migrate_group[group], new_dir_val);
#endif

	while (old_dir != old_dir_end) {
		l_depth = get_two_eh_dir_entry(old_dir, &ent); 
		num = 1UL << (g_depth - l_depth);

		for (i = 0; i < num; ++i) {
			new_dir[i].ent1 = ent.ent1;
			new_dir[i].ent2 = ent.ent2;
		}

		t_ent = set_eh_seg_migrate(ent.ent1);

		if (unlikely(!cas_bool(&(old_dir->ent1), ent.ent1, t_ent)))
			continue;

		new_dir += num;
		old_dir += (num >> EH_EXTENSION_STEP);
	}

	release_fence();
	WRITE_ONCE(eh_group[group], new_dir_val);

	return 0;
}




static void eh_migrate_segment(struct eh_bucket *old_bucket,
							struct eh_segment *new_two_seg,
							int l_depth) {
	EH_KV_ITEM ent_cachebatch[PER_CACHELINE_EH_KV_ITEM];
	EH_KV_ITEM *kv;
	struct eh_bucket *target_bucket;
	u16 count[4] = {0, 0, 0, 0};
	int i, j, k, n, buck_num;

	kv = &(old_bucket->kv[0]);
	target_bucket = &new_two_seg->bucket[0];

	for (buck_num = 0; buck_num < (EH_BUCKET_NUM >> 1); ++buck_num) {
		for (i = 0; i < EH_PER_BUCKET_CACHELINE; ++i) {
			j = 0;
			
			while (j < PER_CACHELINE_EH_KV_ITEM) {
				ent_cachebatch[j] = READ_ONCE(*kv);

				if (unlikely(!eh_entry_valid(ent_cachebatch[j]))) {
					if (likely(cas_bool(kv, 0, EH_END_ENTRY))) {
						kv = &(old_bucket[buck_num + 1].kv[0]);
						i = EH_PER_BUCKET_CACHELINE - 1;
						goto prefetch_kv_from_eh_entry;
					}
					continue;
				}

				if (likely(cas_bool(kv, ent_cachebatch[j], set_eh_entry_split(ent_cachebatch[j])))) {
					prefech_r0((void*)eh_entry_kv_addr(ent_cachebatch[j]));
					++j;
					++kv;
				}
			}
		
		prefetch_kv_from_eh_entry :
			if (buck_num != (EH_BUCKET_NUM >> 1) - 1 || i != EH_PER_BUCKET_CACHELINE - 1)
				prefech_r0((void*)kv);

			for (k = 0; k < j; ++k) {
				u64 ent = ent_cachebatch[k];
				struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(ent);

				if (deleted_kv(tmp_kv))
					delay_chunk_to_rcpage(tmp_kv);
				else {
					n = specific_interval_kv_hashed_key(tmp_kv, 
												eh_entry_fingerprint16(ent), 
												l_depth + EH_BUCKET_INDEX_BIT - 1, 2);
					target_bucket[n].kv[count[n]] = ent;
					count[n] = count[n] + 1;
				}
			}	
		}

		for (n = 0; n < 4; ++n) {
			k = count[n];
			memset(&(target_bucket[n].kv[k]), 0, (EH_PER_BUCKET_KV_NUM - k) * EH_KV_ITEM_SIZE);
			count[n] = 0;
		}

		target_bucket += 4; 
	}
}

__attribute__((always_inline, optimize("unroll-loops")))
static void clean_range_fh_bucket_batching(u64 hashed_key) {
	u64 header;
	struct fh_bucket *bucket;
	int i;

	hashed_key = hashed_key & ~((1UL << (EH_BUCKET_INDEX_BIT + 64 - FH_BUCKET_NUM_BIT)) - 1);
	bucket = get_fh_bucket(hashed_key);

	for (i = 0; i < (1 << EH_BUCKET_INDEX_BIT); ++i) {
		while (1) {
			header = READ_ONCE(bucket->header);

			if (unlikely(!fh_bucket_batching(header)))
				break;

			if (unlikely(fh_bucket_locked(header))) {
				spin_fence();
				continue;
			}

			if (likely(cas_bool(&bucket->header, header, clear_fh_bucket_batching(header))))
				break;
		}

		bucket += 1;
	}
	
}

int split_eh_segment(struct eh_bucket *bucket,
					struct eh_segment *new_two_seg,
					u64 hashed_key, int l_depth) {
	uintptr_t eh_group_ent;
	u64 header;
	struct eh_dir_entry *dir_ent;
	struct fh_bucket *bucket_f = NULL;
	int g_depth, ext, group = eh_group_index(hashed_key);

redo_split_eh_segment :
	eh_group_ent = READ_ONCE(eh_group[group]);
	acquire_fence();
	g_depth = eh_dir_depth(eh_group_ent);
	dir_ent = (struct eh_dir_entry *)extract_eh_dir_addr(eh_group_ent);

	if (l_depth == g_depth) {
	#if BACKGROUNG_THREAD_NUM > 1
		if (eh_group_ent != READ_ONCE(eh_migrate_group[group]))
			return 1;
	#endif
		ext = extend_eh_directory(dir_ent, eh_group_ent, g_depth, group);

		if (unlikely(ext == -1))
			return -1;

	#if BACKGROUNG_THREAD_NUM > 1
		if (ext == 1)
			return 1;
	#endif

		delay_page_to_rcpage(dir_ent, g_depth + (EH_DIR_ENTRY_SIZE_SHIFT - EH_GROUP_BIT));
		goto redo_split_eh_segment;
	}

	prefech_r0(bucket);
	dir_ent += eh_dir_floor_index(hashed_key, l_depth, g_depth);
	prefech_r0(dir_ent);
	
	eh_migrate_segment(bucket, new_two_seg, l_depth);

	/*if (unlikely(l_depth == FH_BUCKET_NUM_BIT - EH_BUCKET_INDEX_BIT)) {
		bucket_f = get_fh_bucket(hashed_key);
		header = lock_fh_bucket(bucket_f);
	}*/

	if (unlikely(l_depth == FH_BUCKET_NUM_BIT - EH_BUCKET_INDEX_BIT))
		clean_range_fh_bucket_batching(hashed_key);
		
	update_eh_dir_entry(dir_ent, new_two_seg, hashed_key, group, l_depth, g_depth);

	/*if (unlikely(bucket_f))
		unlock_fh_bucket(bucket_f, clear_fh_bucket_batching(header));*/

	delay_page_to_rcpage(bucket, EH_SEGMENT_SIZE_BIT - 1);

	return 0;
}