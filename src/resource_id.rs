use std::{cmp, fmt::Display};

use prost::Message;

use crate::proto::build::bazel::remote::execution::v2::{
    Action, Command, Digest, Directory, Platform,
};

#[derive(Clone, Debug)]
pub struct ResourceData {
    pub writer_uuid: String,
    pub data: Vec<u8>,
    // pub completed: bool,
    // TODO: include a ttl
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct SHA256 {
    pub hash: [u8; 32],
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct ResourceId {
    pub length: u64,
    pub hash: SHA256,
}

impl Display for ResourceData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Action
        // Command
        // Directory
        let action = Action::decode(self.data.as_slice());
        if action.is_ok() {
            return write!(
                f,
                "ResourceData of length {} with [{:?}]",
                self.data.len(),
                action
            );
        }
        let command = Command::decode(self.data.as_slice());
        if command.is_ok() {
            return write!(
                f,
                "ResourceData of length {} with [{:?}]",
                self.data.len(),
                command
            );
        }
        let directory = Directory::decode(self.data.as_slice());
        if directory.is_ok() {
            return write!(
                f,
                "ResourceData of length {} with [{:?}]",
                self.data.len(),
                directory
            );
        }
        let elms_to_show = cmp::min(self.data.len(), 8);
        let elms_print: Vec<String> = self.data[0..elms_to_show]
            .into_iter()
            .map(|i| i.to_string())
            .collect();
        write!(
            f,
            "ResourceData of length {} with [{}]",
            self.data.len(),
            elms_print.join(", ")
        )
    }
}

impl Display for SHA256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.hash))
    }
}

impl Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.hash, self.length)
    }
}

impl From<&str> for SHA256 {
    fn from(hash: &str) -> Self {
        let hash_vec = hex::decode(hash).unwrap();
        assert_eq!(hash_vec.len(), 32);

        let mut chash: [u8; 32] = [0; 32];
        for (i, b) in hash_vec.iter().enumerate() {
            chash[i] = *b;
        }
        SHA256 { hash: chash }
    }
}

impl From<String> for SHA256 {
    fn from(hash: String) -> Self {
        hash.as_str().into()
    }
}

impl From<SHA256> for String {
    fn from(hash: SHA256) -> Self {
        hex::encode(hash.hash)
    }
}

impl From<Digest> for ResourceId {
    fn from(digest: Digest) -> Self {
        ResourceId {
            length: digest.size_bytes.try_into().unwrap(),
            hash: digest.hash.into(),
        }
    }
}

impl From<ResourceId> for Digest {
    fn from(resource_id: ResourceId) -> Self {
        Digest {
            hash: resource_id.hash.into(),
            size_bytes: resource_id.length.try_into().unwrap(),
        }
    }
}

impl TryFrom<String> for ResourceId {
    type Error = String; // TODO: This is bad

    fn try_from(resource_name: String) -> Result<Self, Self::Error> {
        let resources: Vec<&str> = resource_name.split('/').collect();
        if resources[0] != "blobs" {
            return Err("First word was not blobs".to_string());
        }
        let length = resources[2].parse();
        if length.is_err() {
            return Err("Could not convert length".to_string());
        }

        Ok(ResourceId {
            length: length.unwrap(),
            hash: resources[1].into(),
        })
    }
}

impl ResourceId {
    pub fn from_resource_name(resource_name: &String) -> (ResourceId, String) {
        // resource_name: "uploads/e57a73ff-744e-4fa3-b9af-b2b384589149/blobs/639b6acdc55bebb727fb0aa19230fc3d548b62e9fc9a0eb9deda55f4c352de32/1146", write_offset: 0, finish_write: true, data = []
        let resources: Vec<&str> = resource_name.split('/').collect();
        assert_eq!(
            resources.len(),
            5,
            "resource_name: {:?}, split: {:?}",
            resource_name,
            resources
        );

        (
            ResourceId {
                length: resources[4].parse().unwrap(),
                hash: resources[3].into(),
            },
            resources[1].to_string(),
        )
    }
}
