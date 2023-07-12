use std::fs::OpenOptions;
use crate::{MeteData, MiniBaseResult};
use crate::node::{Node, NodeType};
use crate::page::{LeafPage, Page, Pager};

pub trait Operate {
    fn put(&self, key: &str, value: &str) -> MiniBaseResult<()>;
    fn get(&self, key: &str) -> MiniBaseResult<Option<String>>;
    fn scan(&self, begin: &str, end: &str) -> MiniBaseResult<Vec<String>>;
    fn remove(&self, key: &str) -> MiniBaseResult<bool>;
}

pub struct Controller {
    mete_data: MeteData,
    root_node: Node,
}

impl Controller {
    pub(crate) fn new(mete_data: MeteData) -> MiniBaseResult<Controller> {
        let data_file_path = mete_data.get_data_file_path();
        let data_file = OpenOptions::new().read(true).write(true).create(true).open(data_file_path.as_str())?;
        data_file.set_len(mete_data.page_size as u64)?;
        let root_node = Node::new(&data_file, 0, mete_data.page_size, NodeType::Leaf)?;
        Ok(Controller { mete_data, root_node })
    }

    pub(crate) fn from(mete_data: MeteData) -> MiniBaseResult<Controller> {
        let data_file_path = mete_data.get_data_file_path();
        let data_file = OpenOptions::new().read(true).write(true).create(true).open(data_file_path.as_str())?;
        let root_page_offset = mete_data.get_root_page_offset();
        let root_page = Page::new(&data_file, root_page_offset, mete_data.page_size)?;
        let page_header = root_page.read_u8(0);
        let node_type = if page_header == LeafPage::HEADER {
            NodeType::Leaf
        } else {
            NodeType::Inner
        };
        let root_node = Node::from(&data_file, 0, mete_data.page_size, node_type)?;
        Ok(Controller { mete_data, root_node })
    }
}

impl Operate for Controller {
    fn put(&self, key: &str, value: &str) -> MiniBaseResult<()> {
        self.root_node.put(key, value)
    }

    fn get(&self, key: &str) -> MiniBaseResult<Option<String>> {
        &self.root_node.get(key)
    }

    fn scan(&self, begin: &str, end: &str) -> MiniBaseResult<Vec<String>> {
        todo!()
    }

    fn remove(&self, key: &str) -> MiniBaseResult<bool> {
        todo!()
    }
}