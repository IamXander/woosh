// mod memory_store;

use crate::proto::build::bazel::remote::execution::v2::ActionResult;
use crate::resource_id::{ResourceData, ResourceId};
use std::collections::HashMap;

#[derive(Default)]
pub struct MemoryStore {
    cas: HashMap<ResourceId, ResourceData>,
    action_cache: HashMap<ResourceId, ActionResult>,
    upload_cache: HashMap<String, Vec<u8>>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore {
            cas: HashMap::new(),
            action_cache: HashMap::new(),
            upload_cache: HashMap::new(),
        }
    }

    pub fn get_action_cache(&self, resource_id: &ResourceId) -> Option<&ActionResult> {
        return self.action_cache.get(resource_id);
    }

    pub fn set_action_cache(&self, resource_id: ResourceId, action_result: ActionResult) {
        self.action_cache.insert(resource_id, action_result);
    }

    pub fn get_write_status(&mut self, resource_name: String) -> (u64, bool) {
        let (res_id, writer_uuid) = ResourceId::from_resource_name(&resource_name);
        let cas_val = self.cas.get(&res_id);
        if cas_val.is_some() && cas_val.unwrap().writer_uuid == writer_uuid {
            return (cas_val.unwrap().data.len().try_into().unwrap(), true);
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
        self.cas.insert(
            res_id,
            ResourceData {
                data: data,
                writer_uuid: writer_uuid,
            },
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
                println!(
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
            println!(
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
}
