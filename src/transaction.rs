use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TransactionType {
    Chargeback,
    Deposit,
    Dispute,
    Resolve,
    Withdrawal,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Transaction {
    r#type: TransactionType,
    client: u32,
    tx: u32,
    amount: Option<f32>,
}

impl Transaction {
    fn new(r#type: TransactionType, client: u32, tx: u32, amount: Option<f32>) -> Result<Transaction> {
        Ok(Transaction {
            r#type,
            client,
            tx,
            amount,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, io::Read};

    fn do_vecs_match<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
        //TODO: What happens with NaN?
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }

    #[test]
    fn test_deserialize_single_transaction() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0";
        let mut rdr = csv::Reader::from_reader(input.as_bytes());

        let mut output = Vec::<Transaction>::new();
        for result in rdr.deserialize() {
            let transaction: Transaction = result.unwrap();
            output.push(transaction);
        }

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }

    #[test]
    fn test_deserialize_set1() {
        let mut file = File::open("data/set1.csv").unwrap();
        let mut input = String::new();
        file.read_to_string(&mut input).unwrap();

        let mut rdr = csv::Reader::from_reader(input.as_bytes());

        let mut output = Vec::<Transaction>::new();
        for result in rdr.deserialize() {
            let transaction: Transaction = result.unwrap();
            output.push(transaction);
        }

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Deposit, 2, 2, Some(2.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 3, Some(2.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Withdrawal, 1, 4, Some(1.5)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Withdrawal, 2, 5, Some(3.0)).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }

    #[test]
    fn test_deserialize_set2() {
        let mut file = File::open("data/set2.csv").unwrap();
        let mut input = String::new();
        file.read_to_string(&mut input).unwrap();

        let mut rdr = csv::Reader::from_reader(input.as_bytes());

        let mut output = Vec::<Transaction>::new();
        for result in rdr.deserialize() {
            let transaction: Transaction = result.unwrap();
            output.push(transaction);
        }

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Withdrawal, 1, 2, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Dispute, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Resolve, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Chargeback, 1, 1, None).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }

    #[test]
    fn test_deserialize_with_whitespace() {
        let mut file = File::open("data/set_whitespace.csv").unwrap();
        let mut input = String::new();
        file.read_to_string(&mut input).unwrap();
        input.retain(|c| c != ' ');

        let mut rdr = csv::Reader::from_reader(input.as_bytes());

        let mut output = Vec::<Transaction>::new();
        for result in rdr.deserialize() {
            let transaction: Transaction = result.unwrap();
            output.push(transaction);
        }

        let mut expected_output = Vec::<Transaction>::new();
        expected_output.push(Transaction::new(TransactionType::Deposit, 1, 1, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Withdrawal, 1, 2, Some(1.0)).unwrap());
        expected_output.push(Transaction::new(TransactionType::Dispute, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Resolve, 1, 1, None).unwrap());
        expected_output.push(Transaction::new(TransactionType::Chargeback, 1, 1, None).unwrap());
        assert!(do_vecs_match(&output, &expected_output));
    }
}
