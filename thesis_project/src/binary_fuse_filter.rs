use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use rand::Rng;
use std::time::Instant;
use std::collections::VecDeque;

struct BFFilter {
    b: Vec<u8>,
    h0_seed: u64,
    h1_seed: u64,
    h2_seed: u64,
    c: usize,
    block_size: usize,
}

impl BFFilter {
    fn fingerprint<T: Hash+Clone+Eq>(&self, x: &T) -> u8 {
        let mut s = DefaultHasher::new();
        x.hash(&mut s);
        let hash_value = s.finish();
        let mask = (1 << 7) - 1; // for 7-bit fingerprints aiming for 0.0078 fpr. however theoretical bit/item is the same as 8 bit fingerprint due to datatype.
        ((hash_value >> (64 - 7)) & mask) as u8
    }

    fn hash<T: Hash+Clone+Eq>(&self, item: &T) -> (usize,usize,usize) {
        let mut hasher0 = DefaultHasher::new();
        item.hash(&mut hasher0);
        let hash0 = hasher0.finish();
        let h0=(((self.h0_seed.wrapping_mul(hash0)) >> 32) % (self.c-2*self.block_size) as u64) as usize;

        let mut hasher1 = DefaultHasher::new();
        item.hash(&mut hasher1);
        let hash1 = hasher1.finish();
        let h1=(((self.h1_seed.wrapping_mul(hash1)) >> 32) % self.block_size as u64) as usize + (h0/self.block_size+1)*self.block_size;

        let mut hasher2 = DefaultHasher::new();
        item.hash(&mut hasher2);
        let hash2 = hasher2.finish();
        let h2 =(((self.h2_seed.wrapping_mul(hash2)) >> 32) % self.block_size as u64) as usize + (h0/self.block_size+2)*self.block_size;
        (h0,h1,h2)
    }//reorganized to reduce hash access times. 3 wise binary fuse filter.


    fn map<T: Hash+Clone+Eq>(&self, buffer: Vec<(T,(usize,usize,usize))>) -> (bool, Vec<(T, usize)>) {
        let mut h: Vec<Vec<T>> = vec![Vec::new(); self.c];
        let key_num = buffer.len();
        for x in buffer {
            let (index0,index1,index2)=x.1;
            h[index0].push(x.0.clone());
            h[index1].push(x.0.clone());
            h[index2].push(x.0.clone());
        }

        let mut queue: VecDeque<usize> = VecDeque::new();

        for i in 0..h.len() {
            if h[i].len() == 1 {
                queue.push_back(i);
            }
        }
        let mut stack: Vec<(T, usize)> = Vec::new();
        while let Some(i) = queue.pop_front() {
            if h[i].len() == 1 {
                if let Some(x) = h[i].get(0).cloned(){
                    stack.push((x.clone(), i));
                    let (index0,index1,index2)=self.hash(&x);
                    for &idx in &[index0, index1, index2] {
                        if let Some(pos) = h[idx].iter().position(|item| *item == x) {
                            h[idx].remove(pos);
                        }
                        if h[idx].len() == 1 {
                            queue.push_back(idx);
                        }
                    }
                }
            }
        }
        let mut new_stack = stack.clone();
        new_stack.reverse();//make sure the stack behave like FILO in assign.
        (stack.len() == key_num, new_stack)
    }

    fn assign<T:Hash+Clone+Eq>(&mut self, stack: Vec<(T, usize)>) {
        for (x, i) in stack {
            let (index0,index1,index2)=self.hash(&x);
            let fp = self.fingerprint(&x);
            self.b[i] = 0;
            self.b[i] = fp ^ self.b[index0] ^ self.b[index1] ^ self.b[index2];
        }
    }

    fn contains<T:Hash+Clone+Eq>(&self, key: &T) -> bool {
        let fp = self.fingerprint(key); // Calculate fingerprint of the key
        let (h0_index,h1_index,h2_index) = self.hash(key);
        // XOR the values stored at these indices in array `B`
        let computed_fp = self.b[h0_index] ^ self.b[h1_index] ^ self.b[h2_index];
        // Return true if the computed fingerprint matches the key's fingerprint
        computed_fp == fp
    }

    fn new<T: Hash+Clone+Eq>(original_keys: & [T]) -> Self {
        let mut rng = rand::thread_rng();
        let n = original_keys.len();
        let block_size = (4.8*(n as f64).powf(0.58)) as usize;
        let c = (((1.125*n as f64).floor()/block_size as f64).ceil() * block_size as f64) as usize; //make sure it can be divided by block size.
        loop {
            let mut filter = BFFilter {
                b: vec![0; c],
                h0_seed:rng.gen::<u64>() | 1,
                h1_seed:rng.gen::<u64>() | 1,
                h2_seed:rng.gen::<u64>() | 1,
                c,
                block_size,
            };
            let mut buffer: Vec<(&T,(usize,usize,usize))> = Vec::new();
            for key in original_keys{
                buffer.push((key,filter.hash(key)));
            }
            buffer.sort_by_cached_key(|key_hash_pair| key_hash_pair.1.0/filter.block_size);
            //store the hash value in a buffer to reduce needed hash access. sorting algorithm is not the best.
            let (success, stack) = filter.map(buffer);
            if success {
                filter.assign(stack);
                return filter;
            }//average retry times more than xor, so more "worse" case can happen. average construction time increases.
            // If not successful, loop will continue and try with new seeds
        }
    }
}

pub fn test_bff(){
    let mut pos_keys: Vec<u32> = (1..=1000000).collect();
    let neg_keys: Vec<u32> = (1000001..=2000000).collect();
    let pos_key_len = pos_keys.len();
    let neg_key_len = neg_keys.len();
    let xor_construction_start=Instant::now();
    let filter = BFFilter::new(pos_keys.as_mut_slice());
    let xor_construction_duration = xor_construction_start.elapsed();
    let bits_per_item =(filter.c*8) as f64/pos_key_len as f64;
    println!("BF theoretical bit/item is {:.2}", bits_per_item);
    println!("BF Filter Construction Time per item for {:?} items: {:?}", pos_key_len,xor_construction_duration/pos_key_len as u32);
    let mut true_positives = 0;
    let mut false_positives = 0;
    let pos_key_check_start=Instant::now();
    for key in pos_keys {
        if filter.contains(&key) {
            true_positives += 1;
        }
    }
    let pos_key_check_duration = pos_key_check_start.elapsed();
    let neg_key_check_start = Instant::now();
    for key in neg_keys{
        if filter.contains(&key){
            false_positives+=1;
        }
    }
    let neg_key_check_duration = neg_key_check_start.elapsed();
    let tpr = true_positives as f64 / pos_key_len as f64;
    let fpr = false_positives as f64 / neg_key_len as f64;
    println!("BF Filter lookup time per item for {:?} inserted items: {:?}", pos_key_len,pos_key_check_duration/pos_key_len as u32);
    println!("BF True Positive Rate (TPR): {:.6}", tpr);
    println!("BF Filter lookup time per item for {:?} non-inserted items: {:?}", neg_key_len,neg_key_check_duration/neg_key_len as u32);
    println!("BF False Positive Rate (FPR): {:.6}", fpr);
}