use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub(crate) fn par_sha256(data: &HashMap<String, Vec<u8>>) -> HashMap<String, Vec<u8>> {
    data.par_iter()
        .map(|(key, bytes)| {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let hash = hasher.finalize();
            (key.to_owned(), hash.to_vec())
        })
        .collect()
}
