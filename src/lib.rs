mod page {
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::fs::File;
    use memmap2::{Mmap, MmapMut, MmapOptions};

    #[derive(Debug)]
    struct MiniBaseError(
        &'static str
    );

    impl Display for MiniBaseError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "mini-base error: {}", self.0)
        }
    }

    impl Error for MiniBaseError {}

    type MiniBaseResult<T> = Result<T, Box<dyn Error>>;

    pub trait Pager {
        fn get_mmap(&self) -> &Mmap;

        fn get_mmap_mut(&mut self) -> &mut MmapMut;

        fn read_u8(&self, offset: usize) -> u8 {
            let mmap = self.get_mmap();
            let data = &mmap[offset..offset + 1];
            data[0]
        }

        fn write_u8(&mut self, offset: usize, value: u8) {
            let mmap_mut = self.get_mmap_mut();
            mmap_mut[offset] = value;
        }

        fn read_u32(&self, offset: usize) -> u32 {
            let mmap = self.get_mmap();
            let data = &mmap[offset..offset + 4];
            u32::from_le_bytes(data.try_into().unwrap())
        }

        fn write_u32(&mut self, offset: usize, value: u32) {
            let mmap_mut = self.get_mmap_mut();
            let data = value.to_le_bytes();
            for i in 0..data.len() {
                mmap_mut[offset + i] = data[i];
            }
        }

        fn read_bytes(&self, offset: usize, length: usize) -> &[u8] {
            let mmap = self.get_mmap();
            &mmap[offset..offset + length]
        }

        fn write_bytes(&mut self, offset: usize, value: &[u8]) {
            let mmap_mut = self.get_mmap_mut();
            for i in 0..value.len() {
                mmap_mut[offset + i] = value[i];
            }
        }

        const HEADER: usize = 0;
        const CAPACITY: usize = 1;
        const DATA_TAIL_OFFSET: usize = 5;
        const DATA_HEAD_OFFSET: usize = 9;
        const SORTED_TABLE: usize = 13;

        fn get_header(&self) -> u8 {
            self.read_u8(Self::HEADER)
        }

        fn update_header(&mut self, value: u8) {
            self.write_u8(Self::HEADER, value)
        }

        fn get_capacity(&self) -> u32 {
            self.read_u32(Self::CAPACITY)
        }

        fn update_capacity(&mut self, value: u32) {
            self.write_u32(Self::CAPACITY, value)
        }

        fn get_data_head_offset(&self) -> usize {
            self.read_u32(Self::DATA_HEAD_OFFSET) as usize
        }

        fn update_data_head_offset(&mut self, value: u32) {
            self.write_u32(Self::DATA_HEAD_OFFSET, value)
        }

        fn get_data_tail_offset(&self) -> usize {
            self.read_u32(Self::DATA_TAIL_OFFSET) as usize
        }

        fn update_data_tail_offset(&mut self, value: u32) {
            self.write_u32(Self::DATA_TAIL_OFFSET, value)
        }

        fn get_free_space(&self) -> u32 {
            (self.get_data_tail_offset() - self.get_data_head_offset()) as u32
        }

        // 获取叶数据的有序列表，返回key_offset的列表，按key的自然序排列
        fn get_sorted_table(&self) -> Vec<usize> {
            let data_head_offset = self.get_data_head_offset();
            let sorted_table_length = (data_head_offset - Self::SORTED_TABLE) / 4;
            let sorted_table_data = self.read_bytes(Self::SORTED_TABLE, sorted_table_length * 4);
            let mut result = Vec::new();
            for i in 0..sorted_table_length {
                let key_offset_data = &sorted_table_data[(i * 4)..(i * 4 + 4)];
                let key_offset = u32::from_le_bytes(key_offset_data.try_into().unwrap());
                result.push(key_offset as usize)
            }
            result
        }

        // 二分查找，返回元素是否存在和元素在有序列表中的索引
        fn binary_search(&self, key: &[u8], sorted_table: &[usize]) -> (bool, usize) {
            if sorted_table.is_empty() {
                return (false, 0);
            }
            let key_position = sorted_table.binary_search_by(|key_offset| {
                let key_size = self.read_u32(*key_offset);
                let key_data = self.read_bytes(*key_offset + 4, key_size as usize);
                key_data.cmp(key)
            });
            match key_position {
                Ok(p) => (true, p),
                Err(e) => (false, e)
            }
        }
    }

    pub trait LeafPager: Pager {}

    pub struct LeafPage {
        mmap: Mmap,
        mmap_mut: MmapMut,
    }

    impl LeafPage {
        pub fn new(file: &File, offset: u64, length: usize) -> MiniBaseResult<LeafPage> {
            let file_metadata = file.metadata()?;
            let file_length = file_metadata.len();
            if file_length < offset + length as u64 {
                file.set_len(offset + length as u64)?;
            }
            let mmap = unsafe { MmapOptions::new().offset(offset).len(length).map(file)? };
            let mmap_mut = unsafe { MmapOptions::new().offset(offset).len(length).map_mut(file)? };
            let mut leaf_page = LeafPage { mmap, mmap_mut };
            leaf_page.init(length);
            Ok(leaf_page)
        }

        pub fn from(file: &File, offset: u64, length: usize) -> MiniBaseResult<LeafPage> {
            let mmap = unsafe { MmapOptions::new().offset(offset).len(length).map(file)? };
            let mmap_mut = unsafe { MmapOptions::new().offset(offset).len(length).map_mut(file)? };
            let leaf_page = LeafPage { mmap, mmap_mut };
            let header = leaf_page.get_header();
            if header != 0b1000_0001 {
                return Err(Box::from(MiniBaseError("header invalid")));
            }
            let capacity = leaf_page.get_capacity();
            if capacity != length as u32 {
                return Err(Box::from(MiniBaseError("capacity invalid")));
            }
            let data_head_offset = leaf_page.get_data_head_offset();
            if data_head_offset < Self::SORTED_TABLE {
                return Err(Box::from(MiniBaseError("data_head_offset invalid")));
            }
            let data_tail_offset = leaf_page.get_data_tail_offset();
            if data_tail_offset < data_head_offset || data_tail_offset > length {
                return Err(Box::from(MiniBaseError("data_tail_offset invalid")));
            }
            Ok(leaf_page)
        }

        // 向叶节点插入数据，返回是否成功，如果key已经关联量数据，value会被覆盖，节点空间不足时会失败
        pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
            let sorted_table = &self.get_sorted_table()[..];
            let (exist, index) = self.binary_search(key, sorted_table);
            return if exist {
                self.override_value(sorted_table, index, value)
            } else {
                self.insert_value(sorted_table, index, key, value)
            };
        }

        pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
            let sorted_table = &self.get_sorted_table()[..];
            let (exist, index) = self.binary_search(key, sorted_table);
            if !exist {
                return None;
            }
            Some(self.get_value(sorted_table, index))
        }

        fn init(&mut self, length: usize) {
            self.update_capacity(length as u32);
            self.update_data_head_offset(Self::SORTED_TABLE as u32);
            self.update_data_tail_offset(length as u32);
            // 1000_0001：初始化完成000_000叶节点
            self.update_header(0b1000_0001);
        }

        fn get_value(&self, sorted_table: &[usize], index: usize) -> &[u8] {
            let key_offset = *sorted_table.get(index).unwrap();
            let key_size = self.read_u32(key_offset);
            let value_offset = self.read_u32(key_offset + 4 + key_size as usize);
            let value_size = self.read_u32(value_offset as usize);
            self.read_bytes((value_offset + 4) as usize, value_size as usize)
        }

        // 覆盖叶节点指定位置的数据，返回是否成功，节点空间不足时会失败
        fn override_value(&mut self, sorted_table: &[usize], index: usize, value: &[u8]) -> bool {
            let old_value = self.get_value(sorted_table, index);
            if old_value == value {
                return true;
            }
            // 判断叶空间是否足够
            if self.get_free_space() < (value.len() + 4) as u32 {
                return false;
            }
            let new_value_offset = self.allocate_space_tail(value.len() + 4).unwrap();
            self.write_u32(new_value_offset, value.len() as u32);
            self.write_bytes(new_value_offset + 4, value);
            self.write_u32(Self::SORTED_TABLE + index * 4, new_value_offset as u32);
            true
        }

        // 向叶插入数据，需要移动数据保证有序列表元素的顺序，节点空间不足时会失败
        fn insert_value(&mut self, sorted_table: &[usize], index: usize, key: &[u8], value: &[u8]) -> bool {
            let required_space = (4 + key.len() + 4 + 4 + value.len() + 4) as u32;
            // 判断叶空间是否足够
            let free_space = self.get_free_space();
            if free_space < required_space {
                return false;
            }
            // 分配value需要的空间
            let new_value_offset = self.allocate_space_tail(4 + value.len()).unwrap();
            // 写入value
            self.write_u32(new_value_offset, value.len() as u32);
            self.write_bytes(new_value_offset + 4, value);
            // 分配key需要的空间
            let new_key_offset = self.allocate_space_tail(4 + key.len() + 4).unwrap();
            // 写入key
            self.write_u32(new_key_offset, key.len() as u32);
            self.write_bytes(new_key_offset + 4, key);
            self.write_u32(new_key_offset + 4 + key.len(), new_value_offset as u32);
            // 更新有序列表
            let new_key_index_offset = self.allocate_space_head(4).unwrap();
            if sorted_table.is_empty() || index == sorted_table.len() - 1 {
                // 叶数据为空或新数据位于末尾，直接插入
                self.write_u32(new_key_index_offset, new_key_offset as u32);
            } else {
                // 需要移动数据，保证顺序
                let move_offset = Self::SORTED_TABLE + 4 * index;
                let bytes_to_move = self.read_bytes(move_offset, (sorted_table.len() - index) * 4);
                let vec = Vec::from(bytes_to_move);
                self.write_bytes(move_offset + 4, &vec);
                self.write_u32(move_offset, new_key_offset as u32);
            }
            true
        }

        // 从头部分配空间
        fn allocate_space_head(&mut self, size: usize) -> Option<usize> {
            let data_head_offset = self.get_data_head_offset();
            let data_tail_offset = self.get_data_tail_offset();
            let new_data_head_offset = data_head_offset + size;
            if new_data_head_offset > data_tail_offset {
                return None;
            }
            self.update_data_head_offset(new_data_head_offset as u32);
            Some(data_head_offset)
        }

        // 从尾部分配空间
        fn allocate_space_tail(&mut self, size: usize) -> Option<usize> {
            let data_head_offset = self.get_data_head_offset();
            let data_tail_offset = self.get_data_tail_offset();
            let new_data_tail_offset = data_tail_offset - size;
            if new_data_tail_offset < data_head_offset {
                return None;
            }
            self.update_data_tail_offset(new_data_tail_offset as u32);
            Some(new_data_tail_offset)
        }
    }

    impl Pager for LeafPage {
        fn get_mmap(&self) -> &Mmap {
            &self.mmap
        }

        fn get_mmap_mut(&mut self) -> &mut MmapMut {
            &mut self.mmap_mut
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::{File, OpenOptions};
    use crate::page::Pager;
    use super::*;

    fn create_test_file(file_name: &str) -> File {
        OpenOptions::new().read(true).write(true).create(true).open(file_name).unwrap()
    }

    fn delete_test_file(file_name: &str) {
        fs::remove_file(file_name).unwrap()
    }

    #[test]
    fn leaf_page_write_read_u8() {
        let mut leaf_page = page::LeafPage::new(&create_test_file("leaf_page_write_read_u8"), 0, 512).unwrap();
        leaf_page.write_u8(0, 1);
        let i = leaf_page.read_u8(0);
        assert_eq!(i, 1);
        delete_test_file("leaf_page_write_read_u8")
    }

    #[test]
    fn leaf_page_write_read_u32() {
        let mut leaf_page = page::LeafPage::new(&create_test_file("leaf_page_write_read_u32"), 0, 512).unwrap();
        leaf_page.write_u32(0, 1);
        let i = leaf_page.read_u32(0);
        assert_eq!(i, 1);
        delete_test_file("leaf_page_write_read_u32")
    }

    #[test]
    fn leaf_page_write_read_bytes() {
        let mut leaf_page = page::LeafPage::new(&create_test_file("leaf_page_write_read_bytes"), 0, 512).unwrap();
        let message = "今天真热";
        let data = message.as_bytes();
        leaf_page.write_bytes(0, data);
        let read = leaf_page.read_bytes(0, data.len());
        assert_eq!(message, String::from_utf8(read.to_vec()).unwrap());
        delete_test_file("leaf_page_write_read_bytes")
    }

    #[test]
    fn leaf_page_insert_get() {
        let page_capacity = 512;
        leaf_page_insert("leaf_page_insert_get", "test", "今天真热", page_capacity);
        leaf_page_get("leaf_page_insert_get", "test", "今天真热", page_capacity);
        delete_test_file("leaf_page_insert_get")
    }

    fn leaf_page_insert(file_name: &str, key: &str, value: &str, page_capacity: usize) {
        let mut leaf_page = page::LeafPage::new(&create_test_file(file_name), 0, page_capacity).unwrap();
        let ok = leaf_page.insert(key.as_bytes(), value.as_bytes());
        assert_eq!(true, ok)
    }

    fn leaf_page_get(file_name: &str, key: &str, expect_value: &str, page_capacity: usize) {
        let leaf_page = page::LeafPage::from(&create_test_file(file_name), 0, page_capacity).unwrap();
        let value = leaf_page.get(key.as_bytes()).unwrap();
        let value = String::from_utf8(Vec::from(value)).unwrap();
        assert_eq!(expect_value.to_string(), value)
    }

    #[test]
    fn leaf_page_override() {
        let page_capacity = 512;
        leaf_page_insert("leaf_page_override", "test", "今天真热", page_capacity);
        leaf_page_get("leaf_page_override", "test", "今天真热", page_capacity);
        leaf_page_insert("leaf_page_override", "test", "今天真热，真滴热", page_capacity);
        leaf_page_get("leaf_page_override", "test", "今天真热，真滴热", page_capacity);
        delete_test_file("leaf_page_override")
    }
}
