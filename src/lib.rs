use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use regex::Regex;
use crate::controller::Controller;
use crate::page::{Page, Pager};

mod page;
mod node;
mod controller;

#[derive(Debug)]
struct MiniBaseError(
    &'static str
);

impl Display for MiniBaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "mini base error: {}", self.0)
    }
}

impl Error for MiniBaseError {}

type MiniBaseResult<T> = Result<T, Box<dyn Error>>;

pub struct MeteData {
    page_size: u32,
    key_max_length: u32,
    value_threshold: u32,
    mete_page: Page,
}

impl MeteData {
    fn get_root_page_offset(&self) -> u32 {
        self.mete_page.read_u32(0)
    }
    fn get_data_file_path(&self) -> String {
        let data_file_path_size = self.mete_page.read_u32(4);
        let data = self.mete_page.read_bytes(4, data_file_path_size as usize);
        String::from_utf8(Vec::from(data)).unwrap()
    }
    fn get_extra_file_path(&self) -> String {
        let data_file_path_size = self.mete_page.read_u32(4);
        let extra_file_path_size = self.mete_page.read_u32((4 + 4 + data_file_path_size) as usize);
        let data = self.mete_page.read_bytes((4 + 4 + data_file_path_size + 4) as usize, extra_file_path_size as usize);
        String::from_utf8(Vec::from(data)).unwrap()
    }

    pub fn controller(self) -> MiniBaseResult<Controller> {
        let data_file_path = self.get_data_file_path();
        if !PathBuf::from(data_file_path).exists() {
            Controller::new(self)
        } else {
            Controller::from(self)
        }
    }
}

pub fn create_schema(data_dir: &str, schema_name: &str, page_size: u32, key_max_length: u32, value_threshold: u32) -> MiniBaseResult<MeteData> {
    if !PathBuf::from(data_dir).exists() {
        return Err(Box::from(MiniBaseError("data_dir not exist")));
    }
    let schema_name_regex = Regex::new(r"[a-z]|[0-9]+?").unwrap();
    if !schema_name_regex.is_match(schema_name) {
        return Err(Box::from(MiniBaseError("schema_name invalid")));
    }
    let format_data_dir = if !data_dir.ends_with('/') {
        String::from(data_dir) + "/"
    } else {
        String::from(data_dir)
    };
    let mete_file_path = format_data_dir.clone() + schema_name + ".m";
    let data_file_path = format_data_dir.clone() + schema_name + ".d";
    let extra_file_path = format_data_dir.clone() + schema_name + ".e";
    if PathBuf::from(mete_file_path.as_str()).exists() {
        return Err(Box::from(MiniBaseError("mete_file already exist")));
    }
    if PathBuf::from(data_file_path.as_str()).exists() {
        return Err(Box::from(MiniBaseError("data_file already exist")));
    }
    if PathBuf::from(extra_file_path.as_str()).exists() {
        return Err(Box::from(MiniBaseError("extra_data_file already exist")));
    }
    let mete_file = OpenOptions::new().read(true).write(true).create(true).open(mete_file_path.as_str())?;
    let mete_page = init_mete_file(&mete_file, data_file_path.as_str(), extra_file_path.as_str())?;
    Ok(MeteData { page_size, key_max_length, value_threshold, mete_page })
}

fn init_mete_file(mete_file: &File, data_file_path: &str, extra_file_path: &str) -> MiniBaseResult<Page> {
    let file_length = 4 + data_file_path.len() + 4 + extra_file_path.len() + 4;
    mete_file.set_len(file_length as u64).unwrap();
    let mut page = Page::new(mete_file, 0, file_length as u32)?;
    page.write_u32(0, 0 as u32);
    page.write_u32(4, data_file_path.len() as u32);
    page.write_bytes(4 + 4, data_file_path.as_bytes());
    page.write_u32(4 + 4 + data_file_path.len(), extra_file_path.len() as u32);
    page.write_bytes(4 + 4 + data_file_path.len() + 4, extra_file_path.as_bytes());
    Ok(page)
}