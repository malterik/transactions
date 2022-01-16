mod input_parser;
mod transaction;

use input_parser::InputParser;
use std::{fs::File, io::Read};
use transaction::Transaction;

fn main() {
    let parser = InputParser::new().unwrap();
    
    let mut file = File::open("data/set1.csv").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    let mut rdr = csv::Reader::from_reader(contents.as_bytes());
    for result in rdr.deserialize() {
        let transaction: Transaction = result.unwrap();
        println!("{:?}", transaction);
    }
}
