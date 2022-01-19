mod input_parser;
mod transaction;

use input_parser::InputParser;
use std::env;


#[tokio::main(flavor="multi_thread", worker_threads=8)]
async fn main() {
    let args : Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Please enter a csv file with transactions");
        return
    }
    let parser = InputParser::new().unwrap();
    parser.parse_transactions(&args[1]).await.unwrap();
}
