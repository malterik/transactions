use crate::transaction::Transaction;
use anyhow::Result;
use itertools::Itertools;
use std::{fs::File, io::Read, time::Instant};

#[derive(Debug)]
pub struct InputParser {}

impl InputParser {
    pub fn new() -> Result<InputParser> {
        Ok(InputParser {})
    }

    pub async fn parse_transactions(self, file: &str) -> Result<Vec<Transaction>> {
        let mut now = Instant::now();
        let mut file = File::open(file)?;
        let mut input = String::new();
        file.read_to_string(&mut input)?;
        println!("Data read {} milliseconds", now.elapsed().as_millis());
        now = Instant::now();

        let tasks: Vec<_> = input
            .lines()
            .skip(1)
            .map(|l| l.to_string())
            .chunks(100000)
            .into_iter()
            .map(|chunk| chunk.collect())
            .map(|chunk: Vec<String>| {
                tokio::spawn(async move {
                    let mut chunk_str: String = chunk.join("\n");
                    chunk_str.retain(|c| c != ' ');
                    let mut csv: String = String::from("type,client,tx,amount\n");
                    if chunk_str.starts_with("type") {
                        panic!("First line was in chunk!!");
                    }
                    csv.push_str(&chunk_str);
                    let mut rdr = csv::Reader::from_reader(csv.as_bytes());
                    let vec: Vec<Transaction> = rdr.deserialize().map(|t| t.unwrap()).collect();
                    vec
                })
            })
            .collect();

        let mut output = vec![];
        for task in tasks {
            output.extend(task.await.unwrap());
        }
        println!(
            "Data deserialized {} milliseconds",
            now.elapsed().as_millis()
        );

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
