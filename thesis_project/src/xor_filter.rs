use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use rand::Rng;
use std::collections::VecDeque;
use std::time::Instant;

struct XorFilter {
    b: Vec<u8>,
    h0_seed: u64,
    h1_seed: u64,
    h2_seed: u64,
    c: usize,
    block_size: usize,
}

impl XorFilter {
    fn fingerprint<T: Hash+Clone+Eq>(&self, x: &T) -> u8 {
        let mut s = DefaultHasher::new();
        x.hash(&mut s);
        let hash_value = s.finish();
        let mask = (1 << 7) - 1; // for 7-bit fingerprints aiming for 0.0078 fpr
        ((hash_value >> (64 - 7)) & mask) as u8
    }

    fn h0<T: Hash+Clone+Eq>(&self, item: &T) -> usize {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        (((self.h0_seed.wrapping_mul(hash)) >> 32) % self.block_size as u64) as usize
    }

    fn h1<T: Hash+Clone+Eq>(&self, item: &T) -> usize {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        (((self.h1_seed.wrapping_mul(hash)) >> 32) % self.block_size as u64) as usize + self.block_size
    }

    fn h2<T: Hash+Clone+Eq>(&self, item: &T) -> usize {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        (((self.h2_seed.wrapping_mul(hash)) >> 32) % self.block_size as u64) as usize + 2 * self.block_size
    }

    fn map<T: Hash+Clone+Eq>(&self, keys: &[T]) -> (bool, Vec<(T, usize)>) {
        let mut h: Vec<Vec<T>> = vec![Vec::new(); self.c];
        for x in keys {
            h[self.h0(x)].push(x.clone());
            h[self.h1(x)].push(x.clone());
            h[self.h2(x)].push(x.clone());
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
                    for &idx in &[self.h0(&x), self.h1(&x), self.h2(&x)] {
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
        new_stack.reverse();//make sure the stack behave like FILO.
        (stack.len() == keys.len(), new_stack)
    }

    fn assign<T:Hash+Clone+Eq>(&mut self, stack: Vec<(T, usize)>) {
        for (x, i) in stack {
            let fp = self.fingerprint(&x);
            self.b[i] = 0;
            self.b[i] = fp ^ self.b[self.h0(&x)] ^ self.b[self.h1(&x)] ^ self.b[self.h2(&x)];
        }
    }

    fn contains<T:Hash+Clone+Eq>(&self, key: &T) -> bool {
        let fp = self.fingerprint(key); // Calculate fingerprint of the key
        let h0_index = self.h0(key); // Get index from h0
        let h1_index = self.h1(key); // Get index from h1
        let h2_index = self.h2(key); // Get index from h2

        // XOR the values stored at these indices in array `B`
        let computed_fp = self.b[h0_index] ^ self.b[h1_index] ^ self.b[h2_index];

        // Return true if the computed fingerprint matches the key's fingerprint
        computed_fp == fp
    }

    fn new<T: Hash+Clone+Eq>(keys: &[T]) -> Self {
        let mut rng = rand::thread_rng();
        let c = (keys.len() as f64*1.23).floor() as usize + 32;
        let block_size = c / 3 ;
        loop {
            let mut filter = XorFilter {
                b: vec![0; c],
                h0_seed:rng.gen::<u64>() | 1,
                h1_seed:rng.gen::<u64>() | 1,
                h2_seed:rng.gen::<u64>() | 1,
                c,
                block_size,
            };

            let (success, stack) = filter.map(keys);
            if success {
                filter.assign(stack);
                return filter;
            }
            // If not successful, loop will continue and try with new seeds
        }
    }
}

pub fn test_xor_filters(){
    let pos_keys: Vec<u32> = (1..=1000000).collect();
    let neg_keys: Vec<u32> = (1000001..=2000000).collect();
    let pos_key_len = pos_keys.len();
    let neg_key_len = neg_keys.len();
    let xor_construction_start=Instant::now();
    let filter = XorFilter::new(&pos_keys);
    let xor_construction_duration = xor_construction_start.elapsed();
    let bits_per_item =(filter.b.len()*8) as f64/pos_key_len as f64;
    println!("Xor Filter Construction Time per item for {:?} items: {:?}", pos_key_len,xor_construction_duration/pos_key_len as u32);
    println!("Xor theoretical bit/item is {:.2}", bits_per_item);
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
    println!("Xor Filter lookup time per item for {:?} inserted items: {:?}", pos_key_len,pos_key_check_duration/pos_key_len as u32);
    println!("Xor True Positive Rate (TPR): {:.6}", tpr);
    println!("Xor Filter lookup time per item for {:?} non-inserted items: {:?}", neg_key_len,neg_key_check_duration/neg_key_len as u32);
    println!("Xor False Positive Rate (FPR): {:.6}", fpr);

}
