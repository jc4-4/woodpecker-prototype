use crate::error::Result;
use same_file::Handle;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Continuously tails a log file from previous offset.
/// Return a buffer of log events aligned by regex.
/// Detect and chase to new file upon end-of-file.
// See the kinesis agent for checkpoints etc. https://github.com/awslabs/amazon-kinesis-agent
pub struct Tailer<'a> {
    path: &'a Path,
    file: File,
    buffer: Vec<u8>,
}

impl Tailer<'_> {
    fn try_new(path: &str, buffer_size: usize) -> Result<Tailer> {
        let path = Path::new(path);
        let file = File::open(path)?;
        Ok(Tailer {
            path,
            file,
            buffer: vec![0; buffer_size],
        })
    }

    fn read(&mut self) -> Result<Option<&[u8]>> {
        let bytes = self.file.read(&mut *self.buffer)?;
        if bytes == 0 {
            Ok(None)
        } else {
            Ok(Some(&self.buffer[0..bytes]))
        }
    }

    fn rotate(&mut self) -> Result<()> {
        // TODO check is_rotated
        self.file = File::open(self.path)?;
        Ok(())
    }

    fn is_rotated(&self) -> Result<bool> {
        let file_handle = Handle::from_file(self.file.try_clone()?)?;
        let path_handle = Handle::from_path(self.path)?;
        Ok(file_handle != path_handle)
    }
}

#[cfg(test)]
mod tests {
    use crate::agent::tailer::Tailer;
    use log::{debug, info};
    use std::env::current_dir;
    use std::fs::{create_dir_all, remove_file, rename, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;
    use std::str::from_utf8;
    use tempfile::NamedTempFile;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn read_file() {
        init();
        let content = b"Mary had a little lamb\nLittle lamb, little lamb";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write(content).unwrap();

        let path_str = temp_file.path().to_str().unwrap();
        debug!("File created at {}", path_str);
        let mut tailer = Tailer::try_new(path_str, 10).unwrap();
        let mut bytes = 0;
        while let Some(v) = tailer.read().unwrap() {
            debug!("length: {} content: {}", v.len(), from_utf8(v).unwrap());
            bytes += v.len();
        }
        assert_eq!(bytes, content.len());
    }

    #[test]
    fn is_rotated() {
        init();
        let mut file1 = NamedTempFile::new().unwrap();
        let path_str = file1.path().to_str().unwrap();
        debug!("File created at {}", path_str);

        let tailer = Tailer::try_new(path_str, 10).unwrap();
        assert!(!tailer.is_rotated().unwrap());

        // Simulate a rotation with rename and create.
        rename(file1.path(), NamedTempFile::new().unwrap().path()).unwrap();
        let _rotated = File::create(path_str).unwrap();
        assert!(tailer.is_rotated().unwrap());
    }

    #[test]
    fn rotate() {
        init();
        let content = b"Mary had a little lamb\nLittle lamb, little lamb";
        let mut file = NamedTempFile::new().unwrap();
        file.write(content).unwrap();

        let path_str = file.path().to_str().unwrap();
        debug!("File created at {}", path_str);

        let mut bytes = 0;
        let mut tailer = Tailer::try_new(path_str, 10).unwrap();
        while let Some(v) = tailer.read().unwrap() {
            debug!("length: {} content: {}", v.len(), from_utf8(v).unwrap());
            bytes += v.len();
        }
        assert_eq!(bytes, content.len());
        assert!(!tailer.is_rotated().unwrap());

        // Simulate a rotation with rename and create.
        let renamed = NamedTempFile::new().unwrap();
        debug!("File renamed to {}", renamed.path().to_str().unwrap());
        rename(file.path(), renamed.path()).unwrap();
        let mut file = File::create(path_str).unwrap();
        debug!("File created at {}", path_str);

        let content2 = b"Mary had a little lamb\nIt's fleece was white as snow";
        file.write(content2).unwrap();
        assert!(tailer.is_rotated().unwrap());

        tailer.rotate().unwrap();
        while let Some(v) = tailer.read().unwrap() {
            debug!("length: {} content: {}", v.len(), from_utf8(v).unwrap());
            bytes += v.len();
        }
        assert_eq!(bytes, content.len() + content2.len());
    }
}
