use std::fs::File;
use memmap2::{Mmap, MmapMut, MmapOptions};
use crate::{MiniBaseError, MiniBaseResult};

fn create_mmap(file: &File, offset: u32, length: u32) -> MiniBaseResult<(Mmap, MmapMut)> {
    let mmap = unsafe { MmapOptions::new().offset(offset as u64).len(length as usize).map(file)? };
    let mmap_mut = unsafe { MmapOptions::new().offset(offset as u64).len(length as usize).map_mut(file)? };
    Ok((mmap, mmap_mut))
}

pub(crate) trait Pager {
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
}

trait DataPager: Pager {
    const HEADER: usize = 0;
    const CAPACITY: usize = 1;
    const PARENT: usize = 5;
    const DATA_HEAD_OFFSET: usize = 9;
    const DATA_TAIL_OFFSET: usize = 13;

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

    fn get_sorted_table_offset(&self) -> usize;

    // 获取叶数据的有序列表，返回key_offset的列表，按key的自然序排列
    fn get_sorted_table(&self) -> Vec<usize> {
        let data_head_offset = self.get_data_head_offset();
        let sorted_table_length = (data_head_offset - self.get_sorted_table_offset()) / 4;
        let sorted_table_data = self.read_bytes(self.get_sorted_table_offset(), sorted_table_length * 4);
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

pub(crate) struct Page {
    mmap: Mmap,
    mmap_mut: MmapMut,
}

impl Pager for Page {
    fn get_mmap(&self) -> &Mmap {
        &self.mmap
    }

    fn get_mmap_mut(&mut self) -> &mut MmapMut {
        &mut self.mmap_mut
    }
}

impl Page {
    pub(crate) fn new(file: &File, offset: u32, length: u32) -> MiniBaseResult<Page> {
        let (mmap, mmap_mut) = create_mmap(file, offset, length)?;
        Ok(Page { mmap, mmap_mut })
    }
}

pub(crate) struct LeafPage {
    mmap: Mmap,
    mmap_mut: MmapMut,
}

impl Pager for LeafPage {
    fn get_mmap(&self) -> &Mmap {
        &self.mmap
    }

    fn get_mmap_mut(&mut self) -> &mut MmapMut {
        &mut self.mmap_mut
    }
}

impl DataPager for LeafPage {
    fn get_sorted_table_offset(&self) -> usize {
        LeafPage::SORTED_TABLE
    }
}

fn common_init<T>(data_pager: &mut T, length: usize, header: u8)
    where T: DataPager {
    data_pager.update_capacity(length as u32);
    data_pager.update_data_head_offset(data_pager.get_sorted_table_offset() as u32);
    data_pager.update_data_tail_offset(length as u32);
    data_pager.update_header(header);
}

fn valid_common_data<T>(data_pager: &T, length: u32, expect_header: u8) -> Option<MiniBaseError>
    where T: DataPager {
    let header = data_pager.get_header();
    if header != expect_header {
        return Some(MiniBaseError("header invalid"));
    }
    let capacity = data_pager.get_capacity();
    if capacity != length {
        return Some(MiniBaseError("capacity invalid"));
    }
    let data_head_offset = data_pager.get_data_head_offset();
    if data_head_offset < data_pager.get_sorted_table_offset() {
        return Some(MiniBaseError("data_head_offset invalid"));
    }
    let data_tail_offset = data_pager.get_data_tail_offset();
    if data_tail_offset < data_head_offset || data_tail_offset > length as usize {
        return Some(MiniBaseError("data_tail_offset invalid"));
    }
    None
}

impl LeafPage {
    const PREVIOUS_PAGE: usize = 17;
    const NEXT_PAGE: usize = 21;
    const SORTED_TABLE: usize = 25;
    pub(crate) const HEADER: u8 = 0b1000_0000;

    pub(crate) fn new(file: &File, offset: u32, length: u32) -> MiniBaseResult<LeafPage> {
        let (mmap, mmap_mut) = create_mmap(file, offset, length)?;
        let mut page = LeafPage { mmap, mmap_mut };
        common_init(&mut page, length as usize, Self::HEADER);
        Ok(page)
    }

    pub(crate) fn from(file: &File, offset: u32, length: u32) -> MiniBaseResult<LeafPage> {
        let (mmap, mmap_mut) = create_mmap(file, offset, length)?;
        let page = LeafPage { mmap, mmap_mut };
        let error = valid_common_data(&page, length, Self::HEADER);
        match error {
            None => Ok(page),
            Some(error) => Err(Box::from(error))
        }
    }

    // 向叶节点插入数据，返回是否成功，如果key已经关联量数据，value会被覆盖，节点空间不足时会失败
    pub(crate) fn insert_key_value(&mut self, key: &[u8], value: &[u8]) -> bool {
        let sorted_table = &self.get_sorted_table()[..];
        let (exist, index) = self.binary_search(key, sorted_table);
        return if exist {
            self.override_value(sorted_table, index, key, value)
        } else {
            self.insert_value(sorted_table, index, key, value)
        };
    }

    pub(crate) fn get_value(&self, key: &[u8]) -> Option<&[u8]> {
        let sorted_table = &self.get_sorted_table()[..];
        let (exist, index) = self.binary_search(key, sorted_table);
        if !exist {
            return None;
        }
        match self.get_value_by_key_offset(*sorted_table.get(index).unwrap()) {
            (false, value) => Some(value),
            (true, _) => None,
        }
    }

    // 删除key value，返回是否成功，key不存在或已删除时失败
    pub(crate) fn delete_value(&mut self, key: &[u8]) -> bool {
        let sorted_table = &self.get_sorted_table()[..];
        let (exist, index) = self.binary_search(key, sorted_table);
        if !exist {
            return false;
        }
        let key_offset = sorted_table.get(index).unwrap();
        self.update_value_delete(*key_offset, true);
        true
    }

    fn get_value_by_key_offset(&self, key_offset: usize) -> (bool, &[u8]) {
        let key_size = self.read_u32(key_offset);
        let deleted = self.read_u8(self.get_value_deleted_position(key_offset, key_size as usize)) == 1;
        let value_offset = self.read_u32(self.get_value_offset_position(key_offset, key_size as usize));
        let value_size = self.read_u32(value_offset as usize);
        let value = self.read_bytes((value_offset + 4) as usize, value_size as usize);
        (deleted, value)
    }

    // 覆盖叶节点指定位置的数据，返回是否成功，节点空间不足时会失败
    fn override_value(&mut self, sorted_table: &[usize], index: usize, key: &[u8], value: &[u8]) -> bool {
        let key_offset = *sorted_table.get(index).unwrap();
        let (deleted, old_value) = self.get_value_by_key_offset(key_offset);
        if old_value == value {
            if deleted {
                self.update_value_delete(key_offset, false);
            }
            return true;
        }
        // 判断叶空间是否足够
        if self.get_free_space() < (value.len() + 4) as u32 {
            return false;
        }
        if deleted {
            self.update_value_delete(key_offset, false);
        }
        let new_value_offset = self.allocate_space_tail(value.len() + 4).unwrap();
        // 写入新的value
        self.write_u32(new_value_offset, value.len() as u32);
        self.write_bytes(new_value_offset + 4, value);
        // 更新key指向的value地址
        self.write_u32(self.get_value_offset_position(key_offset, key.len()), new_value_offset as u32);
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
        let new_value_offset = self.allocate_space_tail(self.get_value_required_space(value)).unwrap();
        // 写入value
        self.write_u32(new_value_offset, value.len() as u32);
        self.write_bytes(new_value_offset + 4, value);
        // 分配key需要的空间
        let new_key_offset = self.allocate_space_tail(self.get_key_required_space(key)).unwrap();
        // 写入key
        self.write_u32(new_key_offset, key.len() as u32);
        self.write_bytes(new_key_offset + 4, key);
        self.write_u32(self.get_value_offset_position(new_key_offset, key.len()), new_value_offset as u32);
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

    fn update_value_delete(&mut self, key_offset: usize, deleted: bool) {
        let deleted = if deleted { 1 } else { 0 };
        let key_size = self.read_u32(key_offset);
        self.write_u8(self.get_value_deleted_position(key_offset, key_size as usize), deleted)
    }

    fn get_value_required_space(&self, value: &[u8]) -> usize {
        4 + value.len()
    }

    fn get_key_required_space(&self, key: &[u8]) -> usize {
        4 + key.len() + 1 + 4
    }

    fn get_value_offset_position(&self, key_offset: usize, key_size: usize) -> usize {
        key_offset + 4 + key_size + 1
    }

    fn get_value_deleted_position(&self, key_offset: usize, key_size: usize) -> usize {
        key_offset + 4 + key_size
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

pub(crate) struct InnerPage {
    mmap: Mmap,
    mmap_mut: MmapMut,
}

impl Pager for InnerPage {
    fn get_mmap(&self) -> &Mmap {
        &self.mmap
    }

    fn get_mmap_mut(&mut self) -> &mut MmapMut {
        &mut self.mmap_mut
    }
}

impl DataPager for InnerPage {
    fn get_sorted_table_offset(&self) -> usize {
        InnerPage::SORTED_TABLE
    }
}

impl InnerPage {
    const LAST_POINTER: usize = 17;
    const SORTED_TABLE: usize = 21;
    pub(crate) const HEADER: u8 = 0b1000_0001;

    pub(crate) fn new(file: &File, offset: u32, length: u32) -> MiniBaseResult<InnerPage> {
        let file_metadata = file.metadata()?;
        let file_length = file_metadata.len();
        if file_length < (offset + length) as u64 {
            file.set_len((offset + length) as u64)?;
        }
        let (mmap, mmap_mut) = create_mmap(file, offset, length)?;
        let mut page = InnerPage { mmap, mmap_mut };
        common_init(&mut page, length as usize, Self::HEADER);
        Ok(page)
    }

    pub(crate) fn from(file: &File, offset: u32, length: u32) -> MiniBaseResult<InnerPage> {
        let (mmap, mmap_mut) = create_mmap(file, offset, length)?;
        let page = InnerPage { mmap, mmap_mut };
        let error = valid_common_data(&page, length, Self::HEADER);
        match error {
            None => Ok(page),
            Some(error) => Err(Box::from(error))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::{File, OpenOptions};

    const PAGE_LENGTH: u32 = 512;

    fn create_test_file(file_name: &str) -> File {
        let file = OpenOptions::new().read(true).write(true).create(true).open(file_name).unwrap();
        file.set_len(PAGE_LENGTH as u64).unwrap();
        file
    }

    fn delete_test_file(file_name: &str) {
        fs::remove_file(file_name).unwrap()
    }

    #[cfg(test)]
    mod test_leaf_page {
        use crate::page::{LeafPage, Pager};
        use super::*;

        #[test]
        fn leaf_page_write_read_u8() {
            let mut leaf_page = LeafPage::new(&create_test_file("leaf_page_write_read_u8"), 0, PAGE_LENGTH).unwrap();
            leaf_page.write_u8(0, 1);
            let i = leaf_page.read_u8(0);
            assert_eq!(i, 1);
            delete_test_file("leaf_page_write_read_u8")
        }

        #[test]
        fn leaf_page_write_read_u32() {
            let mut leaf_page = LeafPage::new(&create_test_file("leaf_page_write_read_u32"), 0, PAGE_LENGTH).unwrap();
            leaf_page.write_u32(0, 1);
            let i = leaf_page.read_u32(0);
            assert_eq!(i, 1);
            delete_test_file("leaf_page_write_read_u32")
        }

        #[test]
        fn leaf_page_write_read_bytes() {
            let mut leaf_page = LeafPage::new(&create_test_file("leaf_page_write_read_bytes"), 0, PAGE_LENGTH).unwrap();
            let message = "今天真热";
            let data = message.as_bytes();
            leaf_page.write_bytes(0, data);
            let read = leaf_page.read_bytes(0, data.len());
            assert_eq!(message, String::from_utf8(read.to_vec()).unwrap());
            delete_test_file("leaf_page_write_read_bytes")
        }

        #[test]
        fn leaf_page_insert_get() {
            let page_capacity = PAGE_LENGTH;
            let file_name = "leaf_page_insert_get";
            let test_file = create_test_file(file_name);

            let mut leaf_page = LeafPage::new(&test_file, 0, page_capacity).unwrap();
            let ok = leaf_page.insert_key_value("test".as_bytes(), "test".as_bytes());
            assert_eq!(true, ok);

            let leaf_page = LeafPage::from(&test_file, 0, page_capacity).unwrap();
            let value = leaf_page.get_value("test".as_bytes()).unwrap();
            let value = String::from_utf8(Vec::from(value)).unwrap();
            assert_eq!("test".to_string(), value);

            let mut leaf_page = LeafPage::from(&test_file, 0, page_capacity).unwrap();
            let ok = leaf_page.insert_key_value("asd".as_bytes(), "asd".as_bytes());
            assert_eq!(true, ok);

            let leaf_page = LeafPage::from(&test_file, 0, page_capacity).unwrap();
            let value = leaf_page.get_value("asd".as_bytes()).unwrap();
            let value = String::from_utf8(Vec::from(value)).unwrap();
            assert_eq!("asd".to_string(), value);

            let mut leaf_page = LeafPage::from(&test_file, 0, page_capacity).unwrap();
            let ok = leaf_page.insert_key_value("songbowen".as_bytes(), "songbowen".as_bytes());
            assert_eq!(true, ok);

            let leaf_page = LeafPage::from(&test_file, 0, page_capacity).unwrap();
            let value = leaf_page.get_value("songbowen".as_bytes()).unwrap();
            let value = String::from_utf8(Vec::from(value)).unwrap();
            assert_eq!("songbowen".to_string(), value);

            delete_test_file(file_name)
        }

        #[test]
        fn leaf_page_override() {
            let page_capacity = PAGE_LENGTH;
            let file_name = "leaf_page_override";

            let mut leaf_page = LeafPage::new(&create_test_file(file_name), 0, page_capacity).unwrap();
            let ok = leaf_page.insert_key_value("test".as_bytes(), "今天真热".as_bytes());
            assert_eq!(true, ok);
            let ok = leaf_page.insert_key_value("test".as_bytes(), "今天真热，真滴热".as_bytes());
            assert_eq!(true, ok);

            let leaf_page = LeafPage::from(&create_test_file(file_name), 0, page_capacity).unwrap();
            let value = leaf_page.get_value("test".as_bytes()).unwrap();
            let value = String::from_utf8(Vec::from(value)).unwrap();
            assert_eq!("今天真热，真滴热".to_string(), value);

            delete_test_file(file_name)
        }

        #[test]
        fn leaf_page_remove() {
            let page_capacity = PAGE_LENGTH;
            let file_name = "leaf_page_remove";

            let mut leaf_page = LeafPage::new(&create_test_file(file_name), 0, page_capacity).unwrap();
            let ok = leaf_page.insert_key_value("test".as_bytes(), "今天真热".as_bytes());
            assert_eq!(true, ok);

            let ok = leaf_page.delete_value("test".as_bytes());
            assert_eq!(true, ok);

            let value = leaf_page.get_value("test".as_bytes());
            assert_eq!(None, value);

            let ok = leaf_page.insert_key_value("test".as_bytes(), "今天真热啊".as_bytes());
            assert_eq!(true, ok);

            let value = leaf_page.get_value("test".as_bytes()).unwrap();
            let value = String::from_utf8(Vec::from(value)).unwrap();
            assert_eq!("今天真热啊".to_string(), value);

            delete_test_file(file_name)
        }
    }
}