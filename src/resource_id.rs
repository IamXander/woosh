#[derive(PartialEq, Eq, Hash)]
pub struct ResourceData {
    pub last_write_uuid: Option<String>,
    pub data: Vec<u8>,
}

#[derive(PartialEq, Eq, Hash)]
pub struct ResourceId {
    pub length: u64,
    pub hash: [u8; 32],
}

pub fn convert_hash(hash: &str) -> [u8; 32] {
    let hash_vec =  hex::decode(hash).unwrap();
    assert_eq!(hash_vec.len(), 32);

    let mut chash: [u8; 32] = [0; 32];
    for (i, b) in hash_vec.iter().enumerate() {
        chash[i] = *b;
    }
    chash
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
    pub fn from_resource_name(resource_name: String) -> ResourceId {
         // resource_name: "uploads/e57a73ff-744e-4fa3-b9af-b2b384589149/blobs/639b6acdc55bebb727fb0aa19230fc3d548b62e9fc9a0eb9deda55f4c352de32/1146", write_offset: 0, finish_write: true, data = []
        let resources: Vec<&str> = resource_name.split('/').collect();
        assert_eq!(resources.len(), 5);
        
        ResourceId{
            length: resources[4].parse().unwrap(),
            hash: convert_hash(resources[3]),
        }
    }
}