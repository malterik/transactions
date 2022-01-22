use crate::transaction::Transaction;
use anyhow::Result;
use std::{fs::File, io::BufRead, io::BufReader};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct InputParser {}

async fn deserialize_transactions(mut chunk: String) -> Vec<Transaction> {
    chunk.retain(|c| c != ' ');
    let mut csv: String = String::from("type,client,tx,amount\n");
    if chunk.starts_with("type") {
        panic!("First line was in chunk!!");
    }
    csv.push_str(&chunk);
    let mut rdr = csv::Reader::from_reader(csv.as_bytes());
    let vec: Vec<Transaction> = rdr.deserialize().map(|t| t.unwrap()).collect();
    vec
}

impl InputParser {
    pub fn new() -> Result<InputParser> {
        Ok(InputParser {})
    }

    pub async fn parse_transactions(self, file: &str) -> Result<Vec<Transaction>> {
        let file = File::open(file)?;
        let file = BufReader::new(file);
        let mut input = String::new();
        let mut tasks: Vec<JoinHandle<Vec<Transaction>>> = vec![];
        for (i, line) in file.lines().enumerate().skip(1) {
            input.extend(line);
            input.push('\n');
            if i % 100000 == 0 {
                tasks.push(tokio::spawn(deserialize_transactions(input.clone())));
                input = String::new();
            }
        }
        // deserialize the rest
        tasks.push(tokio::spawn(deserialize_transactions(input)));
        let mut output = vec![];
        for task in tasks {
            output.extend(task.await.unwrap());
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, process::Command};

    use super::*;
    use crate::transaction::TransactionType;

    fn do_vecs_match<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
        //TODO: What happens with NaN?
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }

    #[tokio::test]
    async fn test_deserialize_set1() {
        let parser = InputParser::new().unwrap();
        let output = parser.parse_transactions("data/set1.csv").await.unwrap();

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Deposit, 2, 2, Some(2.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 3, Some(2.0)).unwrap());
        expected_output
            .push(Transaction::new(TransactionType::Withdrawal, 1, 4, Some(1.5)).unwrap());
        expected_output
            .push(Transaction::new(TransactionType::Withdrawal, 2, 5, Some(3.0)).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }

    #[tokio::test]
    async fn test_deserialize_set2() {
        let parser = InputParser::new().unwrap();
        let output = parser.parse_transactions("data/set2.csv").await.unwrap();

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        expected_output
            .push(Transaction::new(TransactionType::Withdrawal, 1, 2, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Dispute, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Resolve, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Chargeback, 1, 1, None).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }

    #[tokio::test]
    async fn test_deserialize_with_whitespace() {
        let parser = InputParser::new().unwrap();
        let output = parser
            .parse_transactions("data/set_whitespace.csv")
            .await
            .unwrap();

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        expected_output
            .push(Transaction::new(TransactionType::Withdrawal, 1, 2, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Dispute, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Resolve, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Chargeback, 1, 1, None).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_huge_file() {
        let number_of_entries = 3000000;
        if !Path::new("data/huge.csv").exists() {
            println!("huge file does not exist");
            Command::new("python3")
                .arg("generate_file.py")
                .arg(format!("{}", number_of_entries))
                .output()
                .expect("Generation of file failed");
        }
        let parser = InputParser::new().unwrap();
        let output = parser.parse_transactions("data/huge.csv").await.unwrap();

        assert_eq!(output.len(), number_of_entries);
    }
}
