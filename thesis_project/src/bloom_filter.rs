use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::f64;
use rand;
use std::time::Instant;

// Define the BloomFilter struct
pub struct BloomFilter {
    bit_vec: Vec<bool>,
    seeds: Vec<u64>,
    size: usize,
}

impl BloomFilter {
    // Initialize a new BloomFilter with a target false positive rate
    fn new(expected_items: usize) -> BloomFilter {
        // Calculate the size of the bit vector (m) and the number of hash functions (k)
        let false_positive_rate: f64=0.0074;
        let size = (-1f64 * (expected_items as f64) * false_positive_rate.ln() / f64::ln(2f64).powi(2)).ceil() as usize;// This is 'm', the size of the bit array
        let num_hashes = ((size as f64 / expected_items as f64) * f64::ln(2f64)).ceil() as usize;// This is 'k', the number of hash functions
        let seeds = (0..num_hashes).map(|_| rand::random::<u64>() | 1).collect(); // Ensure seeds are odd
        BloomFilter {
            bit_vec: vec![false; size],
            seeds,
            size,
        }
    }


    fn hash<T: Hash>(&self, item: &T, seed: u64) -> usize {//allows a reference to type T.
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();//deal with generic type that implements Hash. so you don't have to map any type of element to an i32 number and then insert/look up things.
        (((seed.wrapping_mul(hash)) >> 32) % self.size as u64) as usize
        //multiply-shift. better distribution to avoid collision.
        // size is not pow of two may lead to un-uniform, but it's the sacrifice to take so that for 1000000 items we don't need 1048576 bits when we have 7 hash functions to achieve 0.007 fpr.
        // real result is 0.0005 more.
    }

    // Add an item to the Bloom filter
    fn add<T: Hash>(&mut self, item: &T) {
        for i in &self.seeds {
            let index = self.hash(item, *i);
            self.bit_vec[index] = true;
        }

    }

    // Check if an item might be in the Bloom filter
    fn contains<T: Hash>(&self, item: &T) -> bool {
        for i in &self.seeds {
            if !self.bit_vec[self.hash(item, *i)] {
                return false;
            }
        }
        true
    }

}

fn test_bloom_f_with_specified_num_of_items(expected_items: usize){
    let expected_items = expected_items;
    let mut filter = BloomFilter::new(expected_items);
    let bits_per_item=filter.size as f64/expected_items as f64;
    println!("Bloom filter theoretical space: bit/item is {:?}", bits_per_item);
    let bloom_f_insertion_start_time = Instant::now();
    for item in 1..=expected_items{
        filter.add(&item);
    }//insert items
    let bloom_f_insertion_duration = bloom_f_insertion_start_time.elapsed();
    println!("Bloom Filter Construction Time per item for {:?} items: {:?}",expected_items,bloom_f_insertion_duration/expected_items as u32);

    let mut bloom_f_false_positive_num=0;
    let bloom_f_neg_query_start_time = Instant::now();
    for item in expected_items+1..=expected_items+expected_items{
        if filter.contains(&item){bloom_f_false_positive_num+=1;}
    }
    let bloom_f_neg_query_duration = bloom_f_neg_query_start_time.elapsed();
    let bloom_fpr= bloom_f_false_positive_num as f64/expected_items as f64;
    println!("Bloom Filter False Positive Rate is ({:?} items) : {:?}",expected_items,bloom_fpr);
    println!("Bloom Filter query Duration per item for {:?} neg items: {:?}",expected_items,bloom_f_neg_query_duration/expected_items as u32);
    let mut bloom_f_true_positive_num=0;
    let bloom_f_pos_query_start_time = Instant::now();
    for item in 1..=expected_items{
        if filter.contains(&item){bloom_f_true_positive_num+=1;}
    }
    let bloom_f_pos_query_duration = bloom_f_pos_query_start_time.elapsed();
    let bloom_tpr= bloom_f_true_positive_num as f64/expected_items as f64;
    println!("Bloom Filter True Positive Rate is ({:?} items) : {:?}",expected_items,bloom_tpr);
    println!("Bloom Filter query Duration per item for {:?} pos items: {:?}",expected_items,bloom_f_pos_query_duration/expected_items as u32);
}

pub fn test_bloom_filters(){
    test_bloom_f_with_specified_num_of_items(996147);

}
