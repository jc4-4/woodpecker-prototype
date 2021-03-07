use regex::Regex;
use regex::bytes;

#[test]
fn example_group() {
    let re = Regex::new("f=(?P<f>\\w+),b=(?P<b>\\w+)").unwrap();
    let text = "f=oo,b=ar";
    let caps = re.captures(text).unwrap();
    assert_eq!("oo", &caps["f"]);
    assert_eq!("ar", &caps["b"]);
    assert_eq!(None, caps.name("nothing"));
}

#[test]
fn example_bytes() {
    let re = bytes::Regex::new("f=(?P<f>\\w+),b=(?P<b>\\w+)").unwrap();
    let bytes = b"f=oo,b=ar";
    let caps = re.captures(bytes).unwrap();

    assert_eq!("oo", std::str::from_utf8(&caps["f"]).unwrap());
    assert_eq!(b"ar", &caps["b"]);
}