use crate::cuckoo_filter::test_cuckoo_filters;
use crate::bloom_filter::test_bloom_filters;
use crate::blocked_bloom_filter::test_blocked_bloom_filters;
use crate::xor_filter::test_xor_filters;
use crate::binary_fuse_filter::test_bff;

mod bloom_filter;
mod blocked_bloom_filter;
mod cuckoo_filter;
mod xor_filter;
mod binary_fuse_filter;



fn main() {
    test_bloom_filters();
    test_blocked_bloom_filters();
    test_cuckoo_filters();
    test_xor_filters();
    test_bff();
}
