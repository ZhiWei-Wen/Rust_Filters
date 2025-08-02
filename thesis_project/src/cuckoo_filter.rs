use rand::{random, Rng};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

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
    }//Ensure the output is the same for each key(item) throughout insertion/lookup/deletion.
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


// test the cuckoo filter.


//The test only works for adding natural numbers from 1 to expected_items for simplicity.
// Test logic needs to be changed if user wants to check for adding different kinds of numbers.

fn compute_mean_and_variance(times: &[Duration]) -> (f64, f64) {
    let times_in_secs: Vec<f64> = times.iter()
        .map(|d| d.as_secs_f64())
        .collect();

    let mean = times_in_secs.iter().sum::<f64>() / times_in_secs.len() as f64;

    let variance = times_in_secs.iter()
        .map(|time| (time - mean).powi(2))
        .sum::<f64>() / times_in_secs.len() as f64;

    (mean, variance)
}




pub fn test_cuckoo_filters() {
    // change SIZE of the cuckoo filter according to ITEM_NUM.
    // SIZE needs to be power of 2. load_factor is in this case is specifically designed as 0.95.
    const ITEM_NUM: i32 = 996147;
    const SIZE:usize = 262144;
    let mut filter = CuckooFilter::new(SIZE); // Adjust size as needed. Power of 2.
    let bits_per_item = (filter.size*BUCKET_SIZE*FINGERPRINT_SIZE) as f64 /ITEM_NUM as f64;
    println!("Cuckoo bit/item is {:?}", bits_per_item);
    
    //insertion check
    let cuckoo_f_insertion_start_time = Instant::now();
    //load factor set to 0.95.
    for i in 1..=ITEM_NUM {
        filter.insert(&i);
    }
    let cuckoo_f_insertion_duration = cuckoo_f_insertion_start_time.elapsed();
    println!("Cuckoo Filter Construction Time per item for {:?} items: {:?}",ITEM_NUM,cuckoo_f_insertion_duration/ITEM_NUM as u32);

    //membership query for inserted items
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
    
    //membership query for non-inserted items
    let mut fp_num = 0;
    let cuckoo_f_lookup_start_time_false = Instant::now();
    for i in ITEM_NUM+1..=2*ITEM_NUM{
        if filter.lookup(&i){fp_num+=1;}
    }
    let cuckoo_f_lookup_duration_false = cuckoo_f_lookup_start_time_false.elapsed();
    let fpr = fp_num as f64/ITEM_NUM as f64;
    println!("Cuckoo Filter lookup time per item for {:?} non-inserted items: {:?}", ITEM_NUM,cuckoo_f_lookup_duration_false/ITEM_NUM as u32);
    println!("Cuckoo Filter FPR is {:?}",fpr);
    
    //deletion time
    let cuckoo_f_delete_start_time = Instant::now();
    for i in 1..=ITEM_NUM{
        filter.delete(&i);
    }
    let cuckoo_f_delete_duration = cuckoo_f_delete_start_time.elapsed();
    println!("Cuckoo Filter deletion time per item for {:?} items: {:?}",ITEM_NUM,cuckoo_f_delete_duration/ ITEM_NUM as u32);
    
    //check if deletion is successful. deleted item should be definitely not in the filter.
    let mut fp2_num = 0;
    for i in 1..=ITEM_NUM{
        if filter.lookup(&i){fp2_num+=1;}
    }
    let fpr2 = fp2_num as f64/ITEM_NUM as f64;
    if fpr2==0f64{
        println!("Cuckoo fpr on inserted items after deletion: {:?}", fpr2)
    }
    //carry out benchmark test for several runs.
    let test_num = 20;
    let mut construct_times: Vec<Duration> = Vec::with_capacity(test_num);
    let mut pos_check_times: Vec<Duration> = Vec::with_capacity(test_num);
    let mut neg_check_times: Vec<Duration> = Vec::with_capacity(test_num);
    let mut deletion_times: Vec<Duration> = Vec::with_capacity(test_num);
    for _ in 0..test_num{
        let mut filter = CuckooFilter::new(SIZE); // Adjust size as needed. Power of 2.
        let cuckoo_f_insertion_start_time = Instant::now();
        //load factor set to 0.95.
        for i in 1..=ITEM_NUM {
            filter.insert(&i);
        }
        let cuckoo_f_insertion_duration = cuckoo_f_insertion_start_time.elapsed();
        construct_times.push(cuckoo_f_insertion_duration);
        //membership query for inserted items
        let cuckoo_f_lookup_start_time = Instant::now();
        let mut tp_num = 0;
        for i in 1..=ITEM_NUM {
            if filter.lookup(&i) {
                tp_num += 1;
            }
        }
        let cuckoo_f_lookup_duration = cuckoo_f_lookup_start_time.elapsed();
        pos_check_times.push(cuckoo_f_lookup_duration);
        let cuckoo_f_lookup_start_time_false = Instant::now();
        for i in ITEM_NUM+1..=2*ITEM_NUM{
            if filter.lookup(&i){fp_num+=1;}
        }
        let cuckoo_f_lookup_duration_false = cuckoo_f_lookup_start_time_false.elapsed();
        neg_check_times.push(cuckoo_f_lookup_duration_false);
        let cuckoo_f_delete_start_time = Instant::now();
        for i in 1..=ITEM_NUM{
            filter.delete(&i);
        }
        let cuckoo_f_delete_duration = cuckoo_f_delete_start_time.elapsed();
        deletion_times.push(cuckoo_f_delete_duration);
    }
    let (construct_mean, construct_variance) = compute_mean_and_variance(&construct_times);
    let (neg_check_mean, neg_check_variance) = compute_mean_and_variance(&neg_check_times);
    let (pos_check_mean, pos_check_variance) = compute_mean_and_variance(&pos_check_times);
    let (del_mean, del_variance) = compute_mean_and_variance(&deletion_times);

    println!("Cuckoo: Construction for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", ITEM_NUM, construct_mean, construct_variance);
    println!("Cuckoo: Negative Check for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", ITEM_NUM, neg_check_mean, neg_check_variance);
    println!("Cuckoo: Positive Check for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", ITEM_NUM, pos_check_mean, pos_check_variance);
    println!("Cuckoo: deletion for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", ITEM_NUM, del_mean, del_variance);

}


