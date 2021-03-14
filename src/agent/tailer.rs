/// Continuously tails a log file from previous offset.
/// Return a buffer of log events aligned by regex.
/// Detect and chase to new file upon end-of-file.
// See the kinesis agent for checkpoints etc. https://github.com/awslabs/amazon-kinesis-agent
pub struct Tailer {

}

#[cfg(test)]
mod tests {
    use log::{debug, info};
    use std::env::current_dir;
    use std::fs::{create_dir_all, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::str::from_utf8;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn read_write() {
        init();
        let content = b"Mary has a little lamb\nLittle lamb,\nlittle lamb";
        let mut file = get_temp_file("foo.txt", content);
        let mut bytes = 0;
        let mut buffer = vec![0; 10];
        while let Ok(i) = file.read(&mut buffer) {
            if i == 0 {
                debug!("No more bytes");
                break;
            } else {
                bytes += i;
                debug!("{}", from_utf8(&buffer[0..i]).unwrap());
            }
        }
        assert_eq!(bytes, content.len());
    }

    /// Returns file handle for a temp file in 'target' directory with a provided content
    pub fn get_temp_file(file_name: &str, content: &[u8]) -> File {
        // build tmp path to a file in "target/debug/testdata"
        let mut path_buf = current_dir().unwrap();
        path_buf.push("target");
        path_buf.push("debug");
        path_buf.push("testdata");
        create_dir_all(&path_buf).unwrap();
        path_buf.push(file_name);

        // write file content
        let mut tmp_file = File::create(path_buf.as_path()).unwrap();
        tmp_file.write_all(content).unwrap();
        tmp_file.sync_all().unwrap();

        // return file handle for both read and write
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path_buf.as_path());
        assert!(file.is_ok());
        file.unwrap()
    }
}
