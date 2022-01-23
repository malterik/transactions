mod input_parser;
mod transaction;
mod transaction_engine;

use input_parser::InputParser;
use transaction_engine::TransactionEngine;

use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Please enter a csv file with transactions");
        return;
    }
    let parser = InputParser::new().unwrap();
    let transactions = parser.parse_transactions(&args[1]).await.unwrap();
    let mut engine = TransactionEngine::new().unwrap();
    engine.process(&transactions);
    engine.print_client_list();
}
