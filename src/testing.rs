use std::any::Any;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::FnOnce;
use std::panic;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::path::Path;
use std::result;

#[derive(Debug)]
pub enum TestFileError {
    IoError(io::Error),
    ThreadError(Box<dyn Any + Send + 'static>),
}

impl From<io::Error> for TestFileError {
    fn from(err: io::Error) -> TestFileError {
        TestFileError::IoError(err)
    }
}

impl From<Box<dyn Any + Send + 'static>> for TestFileError {
    fn from(err: Box<dyn Any + Send + 'static>) -> TestFileError {
        TestFileError::ThreadError(err)
    }
}

pub type Result<T> = result::Result<T, TestFileError>;

pub fn create_test_file<
    P: AsRef<Path> + ?Sized + RefUnwindSafe,
    F: FnOnce(&P, File) -> R + UnwindSafe,
    R,
>(
    path: &P,
    fun: F,
) -> Result<R> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)?;

    let result = panic::catch_unwind(move || fun(path, file));
    fs::remove_file(path)?;
    Ok(result?)
}

pub fn create_two_test_files<
    P1: AsRef<Path> + ?Sized + RefUnwindSafe,
    P2: AsRef<Path> + ?Sized + RefUnwindSafe,
    F: FnOnce(&P1, &P2, File, File) -> R + UnwindSafe,
    R,
>(
    path1: &P1,
    path2: &P2,
    fun: F,
) -> Result<R> {
    let file1 = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path1)?;
    let file2 = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path2)?;
    let result = panic::catch_unwind(move || fun(path1, path2, file1, file2));
    fs::remove_file(path1)?;
    fs::remove_file(path2)?;
    Ok(result?)
}
