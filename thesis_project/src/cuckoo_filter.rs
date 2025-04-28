use rand::{random, Rng};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

const BUCKET_SIZE: usize = 4;
const FINGERPRINT_SIZE: usize = 10; //reduce the likelihood of collisions. instead of 8.
const MAX_NUM_KICKS: usize = 500;

struct CuckooFilter {
    buckets: Vec<Vec<u16>>,
    size: usize,
    seed: u64,
    seed1: u64,
}

impl CuckooFilter {
    fn new(size: usize) -> Self {
        let buckets = vec![Vec::with_capacity(BUCKET_SIZE); size];
        let mut rng = rand::thread_rng();
        let seed = rng.gen::<u64>() | 1;  // Ensure the seed is odd.
        let seed1 = rng.gen::<u64>() | 1;
        CuckooFilter { buckets, size, seed, seed1 }
    }

    fn fingerprint<T:Hash>(&self, x: &T) -> u16 {
        let mut s = DefaultHasher::new();
        x.hash(&mut s);
        let hash_value = s.finish();
        // Apply multiply-shift hashing
        let hashed = self.seed1.wrapping_mul(hash_value);
        let shifted = hashed >> (64 - FINGERPRINT_SIZE); // Right shift to get the top 'FINGERPRINT_SIZE' bits
        (shifted as u16) & ((1 << FINGERPRINT_SIZE) - 1)  // Mask to ensure only 'FINGERPRINT_SIZE' bits are used
    }

    fn hash<T: Hash>(&self, item: &T, seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        (((seed.wrapping_mul(hash)) >> 32) % self.size as u64) as usize//multiply shift
    }//ensure the output is the same for each key(item) throughout insertion/lookup/deletion.
    //This hash function performs better here than in bloom/blocked bloom filters since size is of power of 2.

    fn hash1(&self, x: &i32) -> usize {
        self.hash(x, self.seed)
    }

    fn hash2(&self, i1: usize, f: u16) -> usize {
        let fingerprint_as_i32 = f as i32;
        i1 ^ self.hash(&fingerprint_as_i32, self.seed)
    }// hash(x) xor hash(fingerprint)

    fn insert(&mut self, x: &i32) -> bool {
        let f = self.fingerprint(x);  // Original fingerprint
        let i1 = self.hash1(x);
        let i2 = self.hash2(i1, f);

        if self.buckets[i1].len() < BUCKET_SIZE {
            self.buckets[i1].push(f);
            return true;
        }
        if self.buckets[i2].len() < BUCKET_SIZE {
            self.buckets[i2].push(f);
            return true;
        }

        // Starting with initial indices i1 or i2
        let mut i = if random() { i1 } else { i2 };
        let mut current_fingerprint = f;  // Mutable copy of the fingerprint to be used for swapping

        for _ in 0..MAX_NUM_KICKS {
            let entry = rand::thread_rng().gen_range(0..self.buckets[i].len());
            std::mem::swap(&mut current_fingerprint, &mut self.buckets[i][entry]);  // Swap current_fingerprint with the entry in bucket
            i = self.hash2(i, current_fingerprint);  // Recalculate index using the updated fingerprint

            if self.buckets[i].len() < BUCKET_SIZE {
                self.buckets[i].push(current_fingerprint);  // Push the swapped fingerprint into the new bucket
                return true;
            }
        }
        false
    }


    fn lookup(&self, x: &i32) -> bool {
        let f = self.fingerprint(x);
        let i1 = self.hash1(x);
        let i2 = self.hash2(i1, f);

        self.buckets[i1].contains(&f) || self.buckets[i2].contains(&f)
    }

    fn delete(&mut self, x: &i32) -> bool {
        let f = self.fingerprint(x);
        let i1 = self.hash1(x);
        let i2 = self.hash2(i1, f);

        if let Some(index) = self.buckets[i1].iter().position(|&item| item == f) {
            self.buckets[i1].remove(index);
            return true;
        }

        if let Some(index) = self.buckets[i2].iter().position(|&item| item == f) {
            self.buckets[i2].remove(index);
            return true;
        }

        false
    }
}

const SIZE:usize = 262144; //needs to be power of 2.
const ITEM_NUM: i32 = 996147; //change this accordingly. load_factor is 0.95.

pub fn test_cuckoo_filters() {
    let mut filter = CuckooFilter::new(SIZE); // Adjust size as needed. power of 2.
    let cuckoo_f_insertion_start_time = Instant::now();
    let bits_per_item = (filter.size*BUCKET_SIZE*FINGERPRINT_SIZE) as f64 /ITEM_NUM as f64;
    println!("Cuckoo theoretical bit/item is {:?}", bits_per_item);
    //load factor set to 0.95.
    for i in 1..=ITEM_NUM {
        filter.insert(&i);
    }

    let cuckoo_f_insertion_duration = cuckoo_f_insertion_start_time.elapsed();

    println!("Cuckoo Filter Construction Time per item for {:?} items: {:?}",ITEM_NUM,cuckoo_f_insertion_duration/ITEM_NUM as u32);

    let cuckoo_f_lookup_start_time = Instant::now();
    let mut tp_num = 0;
    for i in 1..=ITEM_NUM {
        if filter.lookup(&i) {
        tp_num += 1;
        }
    }
    let cuckoo_f_lookup_duration = cuckoo_f_lookup_start_time.elapsed();
    let tpr = (tp_num/ITEM_NUM) as f64;
    println!("Cuckoo Filter lookup time per item for {:?} inserted items: {:?}", ITEM_NUM,cuckoo_f_lookup_duration/ITEM_NUM as u32);
    println!("Cuckoo Filter TPR is {:?}",tpr);
    let cuckoo_f_lookup_start_time_false = Instant::now();
    let mut fp_num = 0;
    for i in ITEM_NUM+1..=2*ITEM_NUM{
        if filter.lookup(&i){fp_num+=1;}
    }
    let cuckoo_f_lookup_duration_false = cuckoo_f_lookup_start_time_false.elapsed();
    let fpr = fp_num as f64/ITEM_NUM as f64;
    println!("Cuckoo Filter lookup time per item for {:?} non-inserted items: {:?}", ITEM_NUM,cuckoo_f_lookup_duration_false/ITEM_NUM as u32);
    println!("Cuckoo Filter FPR is {:?}",fpr);
    let cuckoo_f_delete_start_time = Instant::now();
    for i in 1..=ITEM_NUM{
        filter.delete(&i);
    }
    let cuckoo_f_delete_duration = cuckoo_f_delete_start_time.elapsed();
    println!("Cuckoo Filter deletion time per item for {:?} items: {:?}",ITEM_NUM,cuckoo_f_delete_duration/ ITEM_NUM as u32);
    let mut fp2_num = 0;
    for i in 1..=ITEM_NUM{
        if filter.lookup(&i){fp2_num+=1;}
    }
    let fpr2 = fp2_num as f64/ITEM_NUM as f64;
    if fpr2==0f64{
        println!("Cuckoo fpr on inserted items after deletion: {:?}, items deleted successfully", fpr2)
    }

}


