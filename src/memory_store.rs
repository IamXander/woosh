use log::{trace, warn};

use crate::proto::build::bazel::remote::execution::v2::ActionResult;
use crate::resource_id::{ResourceData, ResourceId};
use std::collections::HashMap;
use std::{cmp, fmt};

#[derive(Clone)]
enum CacheEntry {
    AR(ActionResult),
    RD(ResourceData),
}

impl fmt::Display for ActionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for CacheEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheEntry::AR(action_result) => write!(f, "{}", action_result),
            CacheEntry::RD(resource_data) => write!(f, "{}", resource_data),
        }
    }
}

#[derive(Default)]
pub struct MemoryStore {
    // <sha256>/<length> -> data
    cache: HashMap<ResourceId, CacheEntry>,
    // uploads/<uuid>/blobs/<sha256>/<length> -> data
    upload_cache: HashMap<String, Vec<u8>>,
}

impl fmt::Display for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryStore:\n")?;
        for (key, val) in self.cache.iter() {
            write!(f, "\t{}: {}\n", key, val)?;
        }

        Ok(())
    }
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore {
            cache: HashMap::new(),
            upload_cache: HashMap::new(),
        }
    }

    pub fn set_action_cache(&mut self, resource_id: ResourceId, action_result: ActionResult) {
        self.cache
            .insert(resource_id, CacheEntry::AR(action_result));
    }

    pub fn get_write_status(&mut self, resource_name: String) -> (u64, bool) {
        let (res_id, writer_uuid) = ResourceId::from_resource_name(&resource_name);
        let cas_val = self.cache.get(&res_id);
        if cas_val.is_some() {
            if let CacheEntry::RD(cas_val) = cas_val.unwrap() {
                if cas_val.writer_uuid == writer_uuid {
                    return (cas_val.data.len().try_into().unwrap(), true);
                }
            }
        }
        let cache_val = self.upload_cache.get(&resource_name);
        // Nothing has been written yet to this value
        if cache_val.is_none() {
            return (0, false);
        }
        return (cache_val.unwrap().len().try_into().unwrap(), false);
    }

    fn store_data(&mut self, resource_name: &String, data: Vec<u8>) -> u64 {
        let (res_id, writer_uuid) = ResourceId::from_resource_name(resource_name);
        let data_len = data.len().try_into().unwrap();
        self.cache.insert(
            res_id,
            CacheEntry::RD(ResourceData {
                data: data,
                writer_uuid: writer_uuid,
            }),
        );
        return data_len;
    }

    pub fn append_data(
        &mut self,
        resource_name: &String,
        mut data: Vec<u8>,
        write_offset: u64,
        finish_write: bool,
    ) -> Option<u64> {
        // TODO: append to write queue
        // However, for sync we are just using a mutex
        let written_data = self.upload_cache.get_mut(resource_name);
        // New insertion
        if written_data.is_none() {
            // If a write comes in with a non zero offset that we have not seen before
            if write_offset != 0 {
                let (_, uuid) = ResourceId::from_resource_name(resource_name);
                warn!(
                    "A new write attempted with offset {:?} that has uuid {:?}",
                    write_offset, uuid
                );
                return None;
            }
            if finish_write {
                return Some(self.store_data(resource_name, data));
            }
            let data_len = data.len().try_into().unwrap();
            self.upload_cache.insert(resource_name.clone(), data);
            return Some(data_len);
        }

        // Only other option is that we are continuing an insert
        // For some reason a message got lost and we are trying to insert not into the end
        let written_data = written_data.unwrap();
        if write_offset != written_data.len().try_into().unwrap() {
            let (res_id, uuid) = ResourceId::from_resource_name(resource_name);
            trace!(
                "Current offset {:?} append to offset {:?} are different, skipping insertion, resource id: {:?}, uuid: {:?}",
                written_data.len(),
                write_offset,
                res_id,
                uuid
            );
            return None;
        }

        written_data.append(&mut data);
        if !finish_write {
            return Some(written_data.len().try_into().unwrap());
        }
        let erased_data = self.upload_cache.remove(resource_name).unwrap();
        Some(self.store_data(resource_name, erased_data))
    }

    pub fn get_action_cache(&self, resource_id: &ResourceId) -> Option<ActionResult> {
        let cache_entry = self.cache.get(resource_id)?;
        match cache_entry {
            CacheEntry::AR(ar) => return Some(ar.clone()),
            CacheEntry::RD(_) => return None,
        }
    }

    pub fn get_data(&self, resource_id: &ResourceId, offset: u64, limit: u64) -> Option<Vec<u8>> {
        let cache_entry = self.cache.get(&resource_id)?;
        if let CacheEntry::RD(resource_data) = cache_entry {
            let offset: usize = offset.try_into().unwrap();
            let limit: usize = limit.try_into().unwrap();
            if offset >= resource_data.data.len().try_into().unwrap() {
                return Some(vec![]);
            }
            let limit = cmp::min(limit, resource_data.data.len());
            Some(resource_data.data[offset..limit].to_vec())
        } else {
            return None;
        }
    }

    pub fn get_typed_data<MessageType: prost::Message + std::default::Default>(
        &self,
        resource_id: &ResourceId,
    ) -> Option<MessageType> {
        let cache_entry = self.cache.get(&resource_id)?;
        if let CacheEntry::RD(resource_data) = cache_entry {
            let message = MessageType::decode(resource_data.data.as_slice());
            if message.is_err() {
                trace!("Error converting typed data");
                return None;
            }
            Some(message.unwrap())
        } else {
            return None;
        }
    }

    pub fn in_cache(&self, resource_id: &ResourceId) -> bool {
        self.cache.contains_key(&resource_id)
    }
}
