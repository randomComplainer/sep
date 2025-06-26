#[allow(unused_variables)] 
#[allow(dead_code)]
#[allow(unused_imports)]
use bytes::{BufMut, Bytes, BytesMut};

fn main() {
    println!("Hello, world!");
    let arr = [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let mut buf: BytesMut = BytesMut::from(arr.as_slice());

    // println!("buf: {:?}", buf);
    // println!("arr: {:?}", arr);

    test(&mut buf);
    dbg!(buf);
}

fn test(src: &mut BytesMut) {
    let splited = src.split_to(14);
    println!("splited: {:?}", splited);
    println!("src: {:?}", src);
}
