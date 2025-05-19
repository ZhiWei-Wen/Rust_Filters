use std::hash::{Hash,Hasher};
use std::collections::hash_map::DefaultHasher;
use std::time::{Duration, Instant};


const CACHE_LINE_SIZE_BITS: usize = 1024;// 128 bytes M1 Macbook * 8 bits per byte

struct BlockedBloomFilter {
    blocks: Vec<Vec<bool>>,
    num_blocks: usize,
    num_hashes: usize,
    block_size: usize,
    seeds: Vec<u64>,
    total_size: usize
}

impl BlockedBloomFilter {
    fn new(num_elements: usize) -> Self {
        let false_positive_rate:f64 = 0.0074;
        let block_size = CACHE_LINE_SIZE_BITS;
        let total_size = ((-1f64 * (num_elements as f64) * false_positive_rate.ln() / f64::ln(2f64).powi(2)).ceil() * 1.02) as usize;// only 2% space needed to achieve same fpr.
        let num_blocks = ((total_size as f64/block_size as f64).ceil() as usize).max(1);//corner case considered
        let num_hashes = ((total_size/num_elements) as f64 * f64::ln(2f64)).ceil() as usize+1;
        let seeds = (0..num_hashes).map(|_| rand::random::<u64>() | 1).collect();
        let blocks  = vec![vec![false; block_size]; num_blocks];

        BlockedBloomFilter {
            blocks,
            num_blocks,
            num_hashes,
            block_size,
            seeds,
            total_size,
        }
    }
    fn hash_block_index<T: Hash>(&self, item: &T, seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        (((seed.wrapping_mul(hash)) >> 32) % self.num_blocks as u64) as usize//multiply-shift
    }
    fn hash_inside_blocks<T: Hash>(&self, item: &T) -> Vec<usize> {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        let mut hashes = Vec::with_capacity(self.num_hashes-1);
        let hash_space = self.block_size;
        for i in 1..self.num_hashes {
            let hashed_value = (((self.seeds[i].wrapping_mul(hash)) >> 32) % hash_space as u64) as usize;//multiply-shift
            hashes.push(hashed_value);
        }
        hashes
    }

    fn add<T: Hash>(&mut self, item: &T) {
        let hashes = self.hash_inside_blocks(item);
        let block_index = self.hash_block_index(item,self.seeds[0]);
        for hash in hashes{self.blocks[block_index][hash] = true;}
    }

    fn check<T: Hash>(&self, item: &T) -> bool {
        let hashes = self.hash_inside_blocks(item);
        let block_index = self.hash_block_index(item,self.seeds[0]);
        hashes.iter().all(|&index|self.blocks[block_index][index])
    }


}

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
fn test_blocked_bloom_f_with_specified_num_of_items(expected_items: usize){
    
    //carry out single test
    
    let expected_items = expected_items;
    let mut filter = BlockedBloomFilter::new(expected_items);
    let bits_per_item=filter.total_size as f64/expected_items as f64;
    println!("Blocked Bloom filter storage used bit/item is {:?}", bits_per_item);
    let blocked_bloom_f_insertion_start_time = Instant::now();
    for item in 1..=expected_items{
        filter.add(&item);
    }//insert items
    let blocked_bloom_f_insertion_duration = blocked_bloom_f_insertion_start_time.elapsed();
    println!("Blocked Bloom Filter Construction Time per item for {:?} items: {:?}",expected_items,blocked_bloom_f_insertion_duration/expected_items as u32);

    let mut bloom_f_false_positive_num=0;
    let blocked_bloom_f_neg_query_start_time = Instant::now();
    for item in expected_items+1..=expected_items+expected_items{
        if filter.check(&item){bloom_f_false_positive_num+=1;}
    }
    let blocked_bloom_f_neg_query_duration = blocked_bloom_f_neg_query_start_time.elapsed();
    let bloom_fpr= bloom_f_false_positive_num as f64/expected_items as f64;
    println!("Blocked Bloom Filter False Positive Rate is ({:?} items) : {:?}",expected_items,bloom_fpr);
    println!("Blocked Bloom Filter query Duration per item for {:?} negative items: {:?}",expected_items,blocked_bloom_f_neg_query_duration/expected_items as u32);
    let mut bloom_f_true_positive_num=0;
    let blocked_bloom_f_pos_query_start_time = Instant::now();
    for item in 1..=expected_items{
        if filter.check(&item){bloom_f_true_positive_num+=1;}
    }
    let blocked_bloom_f_pos_query_duration = blocked_bloom_f_pos_query_start_time.elapsed();
    let bloom_tpr= bloom_f_true_positive_num as f64/expected_items as f64;
    println!("Blocked Bloom Filter True Positive Rate is ({:?} items) : {:?}",expected_items,bloom_tpr);
    println!("Blocked Bloom Filter query Duration per item for {:?} positive items: {:?}",expected_items,blocked_bloom_f_pos_query_duration/expected_items as u32);
    
    //carry out benchmark test for several runs.
    let test_num = 20;
    let mut construct_times: Vec<Duration> = Vec::with_capacity(test_num);
    let mut pos_check_times: Vec<Duration> = Vec::with_capacity(test_num);
    let mut neg_check_times: Vec<Duration> = Vec::with_capacity(test_num);
    
    for _ in 0..test_num{
        let mut filter = BlockedBloomFilter::new(expected_items);
        
        //time the construction
        let blocked_bloom_f_insertion_start_time = Instant::now();
        for item in 1..=expected_items{
            filter.add(&item);
        }//insert items
        let blocked_bloom_f_insertion_duration = blocked_bloom_f_insertion_start_time.elapsed();
        construct_times.push(blocked_bloom_f_insertion_duration);
        
        //time the lookup time for items not plugged in.
        let blocked_bloom_f_neg_query_start_time = Instant::now();
        for item in expected_items+1..=expected_items+expected_items{
            filter.check(&item);
        }
        let blocked_bloom_f_neg_query_duration = blocked_bloom_f_neg_query_start_time.elapsed();
        neg_check_times.push(blocked_bloom_f_neg_query_duration);
        
        //time the lookup time for items plugged in.
        let blocked_bloom_f_pos_query_start_time = Instant::now();
        for item in 1..=expected_items{
            if filter.check(&item){bloom_f_true_positive_num+=1;}
        }
        let blocked_bloom_f_pos_query_duration = blocked_bloom_f_pos_query_start_time.elapsed();
        pos_check_times.push(blocked_bloom_f_pos_query_duration);
    }
    let (construct_mean, construct_variance) = compute_mean_and_variance(&construct_times);
    let (neg_check_mean, neg_check_variance) = compute_mean_and_variance(&neg_check_times);
    let (pos_check_mean, pos_check_variance) = compute_mean_and_variance(&pos_check_times);

    println!("BBF: Construction for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", expected_items, construct_mean, construct_variance);
    println!("BBF: Negative Check for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", expected_items, neg_check_mean, neg_check_variance);
    println!("BBF: Positive Check for {:?} items in total - Mean: {:.6} sec, Variance: {:.6}", expected_items, pos_check_mean, pos_check_variance);

}   

pub fn test_blocked_bloom_filters(){
    test_blocked_bloom_f_with_specified_num_of_items(996147);
    // match the item number with number of items used in cuckoo filter.
}