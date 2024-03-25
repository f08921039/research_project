#include "fix_hash.h"
#include "per_thread.h"

struct fh_bucket fix_htable[FH_BUCKET_NUM];

__attribute__((always_inline))
static u64 lookback_fh_entry(struct eh_dir_entry *dir_ent, 
							struct kv *kv, u64 hashed_key, 
							u64 fingerprint, int g_depth) {
	dir_ent += eh_dir_index(hashed_key, g_depth);
	return lookup_eh_entry(dir_ent, kv, hashed_key, fingerprint, g_depth);
}

static int writeback_fh_entry_single(struct fh_bucket *bucket,  
							struct eh_dir_entry *dir_ent,
							int g_depth, int ent_num, int ent_map) {
	EH_KV_ITEM kv_array[4];
	struct eh_dir_entry *dir, *next_dir;
	u64 ent, hashed_key, next_hashed_key, next_ent, fing, next_fing;
	u8 ind_arr[4];
	int finish, ind, j, i = 0;

	while (ent_num) {
		ent_num -= 1;
		ind = msb32(ent_map);
		ent_map ^= (1 << ind);

		ent = bucket->ent[ind];
		kv_array[i] = ent;
		prefech_r0((void *)fh_entry_kv_addr(ent));

		ind_arr[i++] = ind;

		if (i == 4) {
		redo_writeback_eh_entry_single :
			ent = kv_array[0];
			fing = fh_entry_fingerprint16(ent);
			hashed_key = combined_kv_hashed_key((struct kv *)fh_entry_kv_addr(ent), fing);

			dir = dir_ent + eh_dir_index(hashed_key, g_depth);

			for (j = 0; j < i; ++j) {
				if (j != i - 1) {
					next_ent = kv_array[j + 1];
					next_fing = fh_entry_fingerprint16(next_ent);
					next_hashed_key = combined_kv_hashed_key(
							(struct kv *)fh_entry_kv_addr(next_ent), next_fing);
					next_dir = dir_ent + eh_dir_index(next_hashed_key, g_depth);
					prefech_r0(next_dir);
				}

				finish = writeback_eh_entry_batch4(dir, hashed_key, fing, g_depth, 1, &ent);
				
				if (finish != 0x0000000F) {
					while (j < i)
						ent_map |= (1 << ind_arr[j++]);

					return ent_map;
				}

				bucket->ent[ind_arr[j]] = clean_fh_entry_kept(ent);

				ent = next_ent;
				dir = next_dir;
				hashed_key = next_hashed_key;
				fing = next_fing;
			}

			if (ent_num == 0)
				return 0;

			i = 0;
		}
	}

	goto redo_writeback_eh_entry_single;
}

static int writeback_fh_entry_batch4(struct fh_bucket *bucket,  
							struct eh_dir_entry *dir_ent,
							u64 hashed_key, int g_depth, 
							int ent_num, int ent_map) {
	u64 fingerprint_simd4 = 0;
	EH_KV_ITEM kv_array[4];
	u64 ent;
	u8 ind_arr[4];
	int ind, finish, i = 0;

	dir_ent += eh_dir_index(hashed_key, g_depth);
	prefech_r0(dir_ent);

	while (ent_num--) {
		ind = msb32(ent_map);
		ent_map ^= (1 << ind);

		ent = bucket->ent[ind];
		kv_array[i] = ent;
		prefech_r0((void *)fh_entry_kv_addr(ent));

		fingerprint_simd4 |= (fh_entry_fingerprint16(ent) << (i << 4));
		ind_arr[i++] = ind;

		if (i == 4) {
		redo_writeback_eh_entry_batch4 :
			finish = writeback_eh_entry_batch4(dir_ent, hashed_key, fingerprint_simd4, g_depth, i, kv_array);

			for (ind = 0; ind < i; ++ind) {
				if (finish & (1 << ind))
					bucket->ent[ind_arr[ind]] = clean_fh_entry_kept(kv_array[ind]);
				else
					ent_map |= (1 << ind_arr[ind]);
			}

			if (ent_num <= 0 || finish != 0x0000000F)
				return ent_map;

			i = 0;
			fingerprint_simd4 = 0;
		}
	}

	goto redo_writeback_eh_entry_batch4;
}

static u64 *traverse_writeback_single_link_bucket(struct kv *kv, u64 fingerprint16, 
												struct fh_bucket *link_bucket,
												struct fh_bucket **last_bucket,
												struct eh_dir_entry *dir_ent,
												int g_depth) {
	u64 header, hashed_key;
	struct fh_bucket *next_bucket;
	int i, index, count, writeback = 1;

traverse_next_link_bucket_single :
	count = 0;
	header = link_bucket->header;
	next_bucket = (struct fh_bucket *)next_fh_bucket(header);

	if (next_bucket)
		prefetch_fh_bucket(next_bucket);

	index = fh_bucket_tail_idx(header);

	for (i = index; i >= 0; --i) {
		u64 entry = link_bucket->ent[i];
		struct kv *ent_kv = (struct kv *)fh_entry_kv_addr(entry);
		prefech_r0(ent_kv);
		
		if ((++count) == 4 || i == 0) {
			while (count) {
				u64 f;
				entry = link_bucket->ent[i + (--count)];
				ent_kv = (struct kv *)fh_entry_kv_addr(entry);
				f = fh_entry_fingerprint16(entry);

				if (fingerprint16 == f && compare_kv_key(kv, ent_kv) == 0)
					return &link_bucket->ent[i + count];

				if (writeback) {
					struct eh_dir_entry *dir;
					hashed_key = combined_kv_hashed_key(ent_kv, f);
					dir = dir_ent + eh_dir_index(hashed_key, g_depth);

					if (writeback_eh_entry_batch4(dir, hashed_key, f, g_depth, 1, &entry) != 0x0000000F)
						writeback = 0;
					else
						link_bucket->header = set_fh_bucket_tail_idx(link_bucket->header, --index);
				}
			}
		}
	}

	if (index == -1) {
		*last_bucket = next_bucket;
		free(link_bucket);
	}

	if (next_bucket) {
		link_bucket = next_bucket;
		goto traverse_next_link_bucket_single;
	}

	return NULL;
}

static u64 *traverse_writeback_batch4_link_bucket(struct kv *kv, u64 hashed_key,
										struct fh_bucket *link_bucket,
										struct fh_bucket **last_bucket, 
										struct eh_dir_entry *dir_ent,
										int g_depth) {
	u64 header, fingerprint_simd4 = 0, fingerprint16 = fh_fingerprint16(hashed_key);
	struct fh_bucket *next_bucket;
	int i, index, count, writeback = 1;

	dir_ent += eh_dir_index(hashed_key, g_depth);
	prefech_r0(dir_ent);

traverse_next_link_bucket_batch :
	count = 0;
	header = link_bucket->header;
	next_bucket = (struct fh_bucket *)next_fh_bucket(header);

	if (next_bucket)
		prefetch_fh_bucket(next_bucket);

	index = fh_bucket_tail_idx(header);

	for (i = index; i >= 0; --i) {
		u64 f;
		u64 entry = link_bucket->ent[i];
		struct kv *ent_kv = (struct kv *)fh_entry_kv_addr(entry);
		f = fh_entry_fingerprint16(entry);

		if (fingerprint16 == f && compare_kv_key(kv, ent_kv) == 0)
			return &link_bucket->ent[i];


		if (writeback) {
			prefech_r0(ent_kv);
			fingerprint_simd4 = ((fingerprint_simd4 << 4) | f);
			
			if ((++count) == 4 || i == 0) {
				int finish = writeback_eh_entry_batch4(dir_ent, hashed_key, 
									fingerprint_simd4, g_depth, count, &link_bucket->ent[i]);
				index -= count;

				if (finish == 0x0000000F) {
					count = 0;
					fingerprint_simd4 = 0;
				} else {
					finish ^= 0x0000000F;
					writeback = 0;

					while (finish) {
						index += 1;
						count = lsb32(finish);
						finish ^= (1 << count);
						link_bucket->ent[index] = link_bucket->ent[i + count];
					}
				}

				link_bucket->header = set_fh_bucket_tail_idx(link_bucket->header, index);
			}
		}
	}

	if (index == -1) {
		*last_bucket = next_bucket;
		free(link_bucket);
	}

	if (next_bucket) {
		link_bucket = next_bucket;
		goto traverse_next_link_bucket_batch;
	}

	return NULL;
}



__attribute__((always_inline))
static int put_kv_link_bucket(u64 entry, struct fh_bucket **last_bucket) {
	u64 header;
	int index;

	if (!(*last_bucket))
		index = FH_BUCKET_SLOT - 1;
	else {
		header = (*last_bucket)->header;
		index = fh_bucket_tail_idx(header);
	}

	if (index == FH_BUCKET_SLOT - 1) {
		struct fh_bucket *new_bucket;

		if (unlikely(malloc_aligned((void **)&new_bucket, FH_BUCKET_SIZE, FH_BUCKET_SIZE)))
			return -1;

		new_bucket->header = make_fh_link_bucket_header(*last_bucket);
		new_bucket->ent[0] = entry;
		*last_bucket = new_bucket;
		return 0;
	}

	index += 1;
	(*last_bucket)->header = set_fh_bucket_tail_idx(header, index);
	(*last_bucket)->ent[index] = entry;
	return 0;
}

__attribute__((always_inline))
static u64 replace_fh_entry(struct fh_bucket *bucket, 
								u64 header, u64 entry,
								int free_map, int index) {
	free_map = (free_map >> index) | ((free_map & ((1 << index) - 1)) << (FH_BUCKET_SLOT - index));
	index += lsb32(free_map);

	if (index > FH_BUCKET_SLOT - 1)
		index -= FH_BUCKET_SLOT;

	bucket->ent[index] = entry;
	return set_fh_bucket_tail_idx(header, index + 1);
}


int put_kv_fh_bucket(struct kv *kv, u64 hashed_key,
							struct fh_bucket *bucket,
						u64 fingerprint16, int group) {
	u64 header, entry;
	u64 new_entry = make_fh_new_entry(kv, fingerprint16);
	struct kv *ent_kv;
	struct fh_bucket *next_bucket;
	struct eh_dir_entry *dir_ent;
	u64 eh_group_ent;
	int batching, g_depth, i, index, ret = 0, kept_num = 0, free_map = fh_bucket_kept_map_mask;

	header = lock_fh_bucket(bucket);
	next_bucket = (struct fh_bucket *)next_fh_bucket(header);
	index = fh_bucket_tail_idx(header);

	if (next_bucket)
		prefetch_fh_bucket(next_bucket);

	for (i = 0; i < FH_BUCKET_SLOT; ++i) {
		entry = bucket->ent[i];

		if (!fh_entry_valid(entry))
			break;

		ent_kv = (struct kv *)fh_entry_kv_addr(entry);

		if (fingerprint16 == fh_entry_fingerprint16(entry) &&
							compare_kv_key(kv, ent_kv) == 0) {
			if (fh_entry_kept(entry))
				free(ent_kv);

			bucket->ent[i] = new_entry;
			goto unlock_for_put_fh_bucket;
		}

		if (fh_entry_kept(entry)) {
			kept_num += 1;
			free_map ^= (1 << i);
		}
	}

	eh_group_ent = READ_ONCE(eh_group[group]);
	dir_ent = (struct eh_dir_entry *)extract_eh_dir_addr(eh_group_ent);
	g_depth = eh_dir_depth(eh_group_ent);
	batching = fh_bucket_batching(header);

	if (kept_num >= FH_ENTRY_WRITEBACK_THREHOLD) {
		int kept_map = free_map ^ fh_bucket_kept_map_mask;

		if (batching)
			kept_map = writeback_fh_entry_batch4(bucket, dir_ent, hashed_key, g_depth, kept_num, kept_map);
		else
			kept_map = writeback_fh_entry_single(bucket, dir_ent, g_depth, kept_num, kept_map);
		
		free_map = kept_map ^ fh_bucket_kept_map_mask;
	}

	if (unlikely(next_bucket)) {
		u64 *last_idx;
		if (batching)
			last_idx = traverse_writeback_batch4_link_bucket(kv, hashed_key, 
										next_bucket, &next_bucket, dir_ent, g_depth);
		else
			last_idx = traverse_writeback_single_link_bucket(kv, fingerprint16, 
										next_bucket, &next_bucket, dir_ent, g_depth);

		header = set_fh_bucket_next_bucket(header, next_bucket);

		if (last_idx) {
			ent_kv = (struct kv *)fh_entry_kv_addr(*last_idx);
			free(ent_kv);

			*last_idx = new_entry;
			goto unlock_for_put_fh_bucket;
		}
	}

	if (likely(free_map))
		header = replace_fh_entry(bucket, header, new_entry, free_map, index);
	else {
		ret = put_kv_link_bucket(new_entry, &next_bucket);
		header = set_fh_bucket_next_bucket(header, next_bucket);
	}

unlock_for_put_fh_bucket :
	unlock_fh_bucket(bucket, header);
	return ret;
}


int get_kv_fh_bucket(struct kv *kv, u64 hashed_key,
							struct fh_bucket *bucket,
							u64 fingerprint16, int group) {
	u64 header, entry;
	struct kv *ent_kv;
	struct fh_bucket *next_bucket;
	struct eh_dir_entry *dir_ent;
	u64 eh_group_ent;
	int i, ret, batching, g_depth, index, free_map = fh_bucket_kept_map_mask;

	header = lock_fh_bucket(bucket);
	next_bucket = (struct fh_bucket *)next_fh_bucket(header);

	if (next_bucket)
		prefetch_fh_bucket(next_bucket);

	for (i = 0; i < FH_BUCKET_SLOT; ++i) {
		entry = bucket->ent[i];

		if (!fh_entry_valid(entry))
			break;

		ent_kv = (struct kv *)fh_entry_kv_addr(entry);

		if (fingerprint16 == fh_entry_fingerprint16(entry) &&
						compare_kv_key(kv, ent_kv) == 0) {
		get_matched_kv_fh_bucket :
			if (unlikely(deleted_kv(ent_kv)))
				ret = -1;
			else {
				copy_kv_val(kv, ent_kv);
				ret = 0;
			}

			unlock_fh_bucket(bucket, header);
			return ret;
		}

		if (fh_entry_kept(entry))
			free_map ^= (1 << i);
	}

	eh_group_ent = READ_ONCE(eh_group[group]);
	dir_ent = (struct eh_dir_entry *)extract_eh_dir_addr(eh_group_ent);
	g_depth = eh_dir_depth(eh_group_ent);

	if (unlikely(next_bucket)) {
		u64 *last_idx;
		batching = fh_bucket_batching(header);

		if (batching)
			last_idx = traverse_writeback_batch4_link_bucket(kv, hashed_key,
										next_bucket, &next_bucket, dir_ent, g_depth);
		else
			last_idx = traverse_writeback_single_link_bucket(kv, fingerprint16,
										next_bucket, &next_bucket, dir_ent, g_depth);

		header = set_fh_bucket_next_bucket(header, next_bucket);

		if (last_idx) {
			ent_kv = (struct kv *)fh_entry_kv_addr(*last_idx);
			goto get_matched_kv_fh_bucket;
		}
	}

	entry = lookback_fh_entry(dir_ent, kv, hashed_key, fingerprint16, g_depth);

	if (!eh_entry_valid(entry))
		ret = -1;
	else {
		ret = 0;
		
		if (likely(free_map)) {
			index = fh_bucket_tail_idx(header);
			header = replace_fh_entry(bucket, header, entry, free_map, index);
		}
	}

	unlock_fh_bucket(bucket, header);
	return ret;
}
