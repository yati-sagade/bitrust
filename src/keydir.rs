use common::{FileID};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::hash::Hash;

// The keydir is the main in-memory lookup table that bitcask uses, and
// KeyDirEntry is an entry in that table (which is keyed by a String key, see
// below). The KeyDirEntry for a given key tells us which file we need to look
// into, along with the offset into that file, and the size of the record we
// must read out.
#[derive(Debug)]
pub struct KeyDirEntry {
    /// Id of the file where this key lives
    pub file_id: FileID,
    /// Size in bytes of the record
    pub record_size: u16,
    /// Byte offset of the data record in the file
    pub record_offset: u64,
    /// local timestamp when this entry was written
    pub timestamp: u64,
}

impl KeyDirEntry {
    pub fn new(
        file_id: FileID,
        record_size: u16,
        record_offset: u64,
        timestamp: u64,
    ) -> KeyDirEntry {
        KeyDirEntry {
            file_id,
            record_size,
            record_offset,
            timestamp,
        }
    }
}

// The keydir is the main in memory data structure in bitcask. All of the
// keyspace (+ some constant size metadata per key) must fit in memory.
//
// During reads, keydirs are consulted to find where on disk to find the record.
// Keydirs are updated after each write.
#[derive(Debug)]
pub struct KeyDir {
    /// mapping from a key to its keydir entry
    pub entries: HashMap<Vec<u8>, KeyDirEntry>,
    rwlock: Arc<RwLock<()>>,
}

impl KeyDir {
    pub fn new() -> KeyDir {
        KeyDir {
            entries: HashMap::new(),
            rwlock: Arc::new(RwLock::new(())),
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, entry: KeyDirEntry) {
        let _write_lock = self.rwlock.write().unwrap();
        self.entries.insert(key, entry);
    }

    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a KeyDirEntry> {
        let _read_lock = self.rwlock.read().unwrap();
        self.entries.get(key)
    }

    pub fn keys<'a>(&'a self) -> Vec<&'a [u8]> {
        let _read_lock = self.rwlock.read().unwrap();
        self.entries.keys().map(|v| &v[..]).collect()
    }

    pub fn update_keydir_entry_if<P>(&mut self,
                                     key: &Vec<u8>,
                                     entry: KeyDirEntry,
                                     pred: P) -> bool
        where for<'r> P: Fn(&'r KeyDirEntry) -> bool
    {
        let _lock = self.rwlock.write().unwrap();
        // Again to work around the stupid borrowck issues here, we delegate
        // out to a function with explicitly borrowed self.entries.
        let entries = &mut self.entries;
        update_hashmap_if(entries, key, entry, pred)
    }
}

fn update_hashmap_if<K, KRef, V, P>(hm: &mut HashMap<K, V>, key: KRef, new_val: V, pred: P) -> bool
    where K: Hash + Eq,
          P: Fn(&V) -> bool,
          KRef: AsRef<K>

{
    hm.get_mut(key.as_ref()).map_or(false, |entry| {
        let should_update = pred(entry);
        if should_update {
            *entry = new_val;
            true
        } else {
            false
        }
    })
}
