use std::fs::File;
use crate::MiniBaseResult;
use crate::page::{InnerPage, LeafPage};

#[derive(PartialEq, Copy, Clone)]
pub(crate) enum NodeType {
    Leaf,
    Inner,
}

pub(crate) struct Node {
    leaf_page: Option<LeafPage>,
    inner_page: Option<InnerPage>,
    node_type: NodeType,
}

impl Node {
    pub(crate) fn new(file: &File, offset: u32, page_size: u32, node_type: NodeType) -> MiniBaseResult<Node> {
        match node_type {
            NodeType::Leaf => {
                let page = LeafPage::new(file, offset, page_size)?;
                Ok(Node { leaf_page: Some(page), inner_page: None, node_type: NodeType::Leaf })
            }
            NodeType::Inner => {
                let page = InnerPage::new(file, offset, page_size)?;
                Ok(Node { leaf_page: None, inner_page: Some(page), node_type: NodeType::Inner })
            }
        }
    }

    pub(crate) fn from(file: &File, offset: u32, page_size: u32, node_type: NodeType) -> MiniBaseResult<Node> {
        match node_type {
            NodeType::Leaf => {
                let page = LeafPage::from(file, offset, page_size)?;
                Ok(Node { leaf_page: Some(page), inner_page: None, node_type: NodeType::Leaf })
            }
            NodeType::Inner => {
                let page = InnerPage::from(file, offset, page_size)?;
                Ok(Node { leaf_page: None, inner_page: Some(page), node_type: NodeType::Inner })
            }
        }
    }

    fn get_type(&self) -> NodeType {
        self.node_type
    }

    pub(crate) fn put(&self, key: &str, value: &str) -> MiniBaseResult<()> {
        todo!()
    }
}