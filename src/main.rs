mod input_parser;
mod transaction;
mod transaction_engine;

use input_parser::InputParser;
use tokio::time::Instant;
use transaction_engine::TransactionEngine;

use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Please enter a csv file with transactions");
        return;
    }
    let mut now = Instant::now();
    let parser = InputParser::new().unwrap();
    let transactions = parser.parse_transactions(&args[1]).await.unwrap();
    println!("File parsed in: {} ms", now.elapsed().as_millis());
    now = Instant::now();

    let mut engine = TransactionEngine::new().unwrap();
    engine.process(&transactions);
    println!("Transactions processed in: {} ms", now.elapsed().as_millis());
    engine.print_client_list();
}
