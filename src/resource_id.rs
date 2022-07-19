use crate::proto::build::bazel::remote::execution::v2::Digest;

#[derive(Clone)]
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

// pub fn convert_hash(hash: &str) -> [u8; 32] {
//     let hash_vec = hex::decode(hash).unwrap();
//     assert_eq!(hash_vec.len(), 32);

//     let mut chash: [u8; 32] = [0; 32];
//     for (i, b) in hash_vec.iter().enumerate() {
//         chash[i] = *b;
//     }
//     chash
// }

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

// impl ResourceData {
//     pub fn from_resource_name(resource_name: String) -> WriteResourceId {
//          // resource_name: "uploads/e57a73ff-744e-4fa3-b9af-b2b384589149/blobs/639b6acdc55bebb727fb0aa19230fc3d548b62e9fc9a0eb9deda55f4c352de32/1146", write_offset: 0, finish_write: true, data = []
//         let resources: Vec<&str> = resource_name.split('/').collect();
//         assert_eq!(resources.len(), 5);

//         WriteResourceId{
//             uuid: resources[1].into(),
//             length: resources[4].parse().unwrap(),
//             hash: convert_hash(resources[3]),
//         }
//     }
// }

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
