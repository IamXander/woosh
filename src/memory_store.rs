// mod lib;

use crate::{google::bytestream::WriteRequest, resource_id::{ResourceId, ResourceData}};
use std::collections::HashMap;


pub struct MemoryStore {
    cas: HashMap<ResourceId, ResourceData>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore{
            cas: HashMap::new()
        }
    }

    pub fn append_data(& mut self, write_request: WriteRequest) {
        // TODO: append to write queue
        let res_id = ResourceId::from_resource_name(write_request.resource_name);
        // if cas.entry(res_id) 
    }
}